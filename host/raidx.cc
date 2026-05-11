#include "bdev_raid.h"

#include "../shared/raidx_ec.h"
#include "../shared/raidx_layout.h"

#include "spdk/env.h"
#include "spdk/log.h"
#include "spdk/string.h"
#include "spdk/thread.h"
#include "spdk/util.h"

struct raidx_info {
    uint64_t stripe_blocks;
    uint64_t total_stripes;
    struct raidx_ec_engine ec;
};

struct raidx_io_channel {
    TAILQ_HEAD(, spdk_bdev_io_wait_entry) retry_queue;
};

struct raidx_write_stripe {
    void **chunk_buffers;
};

struct raidx_request_ctx {
    struct raid_bdev_io *raid_io;
    struct raidx_write_stripe *stripes;
    uint32_t stripe_count;
    uint32_t pending;
    uint64_t total_blocks;
};

struct raidx_partial_chunk {
    uint8_t chunk_idx;
    uint8_t data_index;
    uint64_t req_offset;
    uint64_t req_blocks;
    uint64_t iov_offset;
};

static int
raidx_copy_payload_to_send_wrapper(struct send_wr_wrapper *send_wrapper,
                                   const struct iovec *iov, int iovcnt,
                                   uint64_t iov_offset, uint64_t len)
{
    struct iovec dst_iov;

    if (len > kMsgSize) {
        return -EINVAL;
    }

    dst_iov.iov_base = send_wrapper->buf;
    dst_iov.iov_len = (size_t)len;
    raid_memcpy_iovs(&dst_iov, 1, 0, iov, iovcnt, iov_offset, len);
    return 0;
}

static bool
raidx_trace_layout_enabled(void)
{
    const char *value = getenv("DRAID_RAIDX_TRACE_LAYOUT");

    return value != NULL && value[0] != '\0' && strcmp(value, "0") != 0;
}

static bool
raidx_trace_degraded_enabled(void)
{
    const char *value = getenv("DRAID_RAIDX_TRACE_DEGRADED");

    return value != NULL && value[0] != '\0' && strcmp(value, "0") != 0;
}

struct raidx_stripe_state {
    uint8_t degraded_data_count;
    uint8_t degraded_parity_count;
    uint8_t surviving_parity_count;
    int8_t surviving_parity_slots[kRaidxMaxParityRows];
    uint8_t surviving_parity_rows[kRaidxMaxParityRows];
};

struct raidx_recon_source {
    uint8_t slot;
    uint8_t coeff;
};

struct raidx_recon_plan {
    struct raidx_partial_chunk missing_chunk;
    std::vector<struct raidx_recon_source> sources;
};

static uint8_t
raidx_slot_fragment_index(uint64_t stripe_index, uint8_t total_chunks,
                          uint8_t num_parities, uint8_t slot_index)
{
    int8_t data_index = raidx_chunk_data_index(stripe_index, total_chunks, num_parities, slot_index);

    if (data_index >= 0) {
        return (uint8_t)data_index;
    }

    return (uint8_t)(raidx_num_data_chunks(total_chunks, num_parities) +
                     raidx_slot_parity_row(stripe_index, total_chunks, num_parities, slot_index));
}

static uint8_t
raidx_generator_coeff(uint8_t total_chunks, uint8_t num_parities,
                      uint8_t fragment_index, uint8_t data_index)
{
    uint8_t num_data_chunks = raidx_num_data_chunks(total_chunks, num_parities);

    if (fragment_index < num_data_chunks) {
        return fragment_index == data_index ? 1 : 0;
    }

    return raidx_parity_coeff((uint8_t)(fragment_index - num_data_chunks), data_index);
}

static int
raidx_build_recon_plan(const struct raid_bdev *raid_bdev, uint64_t stripe_index,
                       const struct raidx_partial_chunk *missing_chunk,
                       struct raidx_recon_plan *plan)
{
    uint8_t num_data_chunks = raid_bdev->num_data_chunks;
    std::vector<uint8_t> source_slots;
    std::vector<unsigned char> matrix((size_t)num_data_chunks * num_data_chunks);
    std::vector<unsigned char> invert_matrix((size_t)num_data_chunks * num_data_chunks);

    for (uint8_t slot = 0; slot < raid_bdev->num_base_rpcs && source_slots.size() < num_data_chunks; ++slot) {
        if (raid_bdev->base_rpc_info[slot].degraded || slot == missing_chunk->chunk_idx) {
            continue;
        }
        source_slots.push_back(slot);
    }

    if (source_slots.size() != num_data_chunks) {
        return -ENOTSUP;
    }

    for (uint8_t row = 0; row < num_data_chunks; ++row) {
        uint8_t fragment_index = raidx_slot_fragment_index(stripe_index, raid_bdev->num_base_rpcs,
                                                           raid_bdev->num_parities, source_slots[row]);
        for (uint8_t col = 0; col < num_data_chunks; ++col) {
            matrix[row * num_data_chunks + col] = raidx_generator_coeff(raid_bdev->num_base_rpcs,
                                                                        raid_bdev->num_parities,
                                                                        fragment_index, col);
        }
    }

    if (gf_invert_matrix(matrix.data(), invert_matrix.data(), num_data_chunks) < 0) {
        return -ENOTSUP;
    }

    plan->missing_chunk = *missing_chunk;
    plan->sources.clear();
    for (uint8_t source_idx = 0; source_idx < num_data_chunks; ++source_idx) {
        struct raidx_recon_source source = {};

        source.slot = source_slots[source_idx];
        source.coeff = invert_matrix[missing_chunk->data_index * num_data_chunks + source_idx];
        if (source.coeff != 0) {
            plan->sources.push_back(source);
        }
    }

    return plan->sources.empty() ? -ENOTSUP : 0;
}

static void
raidx_collect_stripe_state(const struct raid_bdev *raid_bdev, uint64_t stripe_index,
                           struct raidx_stripe_state *stripe_state)
{
    memset(stripe_state, 0, sizeof(*stripe_state));
    for (uint8_t i = 0; i < kRaidxMaxParityRows; ++i) {
        stripe_state->surviving_parity_slots[i] = -1;
        stripe_state->surviving_parity_rows[i] = UINT8_MAX;
    }

    for (uint8_t row = 0; row < raid_bdev->num_parities; ++row) {
        uint8_t slot = raidx_parity_slot(stripe_index, raid_bdev->num_base_rpcs, row);

        if (raid_bdev->base_rpc_info[slot].degraded) {
            if (raidx_trace_degraded_enabled()) {
                SPDK_NOTICELOG("RAIDX_DEGRADED stripe=%llu slot=%u role=parity parity_row=%u\n",
                               (unsigned long long)stripe_index, slot, row);
            }
            stripe_state->degraded_parity_count++;
            continue;
        }

        stripe_state->surviving_parity_slots[stripe_state->surviving_parity_count] = slot;
        stripe_state->surviving_parity_rows[stripe_state->surviving_parity_count] = row;
        stripe_state->surviving_parity_count++;
    }

    for (uint8_t slot = 0; slot < raid_bdev->num_base_rpcs; ++slot) {
        if (!raid_bdev->base_rpc_info[slot].degraded) {
            continue;
        }

        if (!raidx_is_parity_slot(stripe_index, raid_bdev->num_base_rpcs,
                                  raid_bdev->num_parities, slot)) {
            if (raidx_trace_degraded_enabled()) {
                SPDK_NOTICELOG("RAIDX_DEGRADED stripe=%llu slot=%u role=data\n",
                               (unsigned long long)stripe_index, slot);
            }
            stripe_state->degraded_data_count++;
        }
    }
}

static bool
raidx_write_stripe_is_supported(const struct raid_bdev *raid_bdev,
                                const struct raidx_stripe_state *stripe_state,
                                bool is_full_stripe_write)
{
    uint8_t degraded_total = stripe_state->degraded_data_count + stripe_state->degraded_parity_count;

    if (degraded_total > raid_bdev->num_parities) {
        return false;
    }

    (void)is_full_stripe_write;
    return true;
}

static int
raidx_map_iov(struct sg_entry *sgl, const struct iovec *iov, int iovcnt,
              uint64_t offset, uint64_t len)
{
    int i;
    size_t off = 0;
    int start_v = -1;
    size_t start_v_off = 0;
    int new_iovcnt = 0;

    for (i = 0; i < iovcnt; i++) {
        if (off + iov[i].iov_len > offset) {
            start_v = i;
            break;
        }
        off += iov[i].iov_len;
    }

    if (start_v == -1) {
        return -1;
    }

    start_v_off = off;

    for (i = start_v; i < iovcnt; i++) {
        new_iovcnt++;

        if (off + iov[i].iov_len >= offset + len) {
            break;
        }
        off += iov[i].iov_len;
    }

    assert(start_v + new_iovcnt <= iovcnt);

    off = start_v_off;
    iov += start_v;

    for (i = 0; i < new_iovcnt; i++) {
        sgl[i].addr = (uint64_t)iov->iov_base + (offset - off);
        sgl[i].len = spdk_min(len, iov->iov_len - (offset - off));

        off += iov->iov_len;
        iov++;
        offset += sgl[i].len;
        len -= sgl[i].len;
    }

    return len == 0 ? new_iovcnt : -1;
}

static void
raidx_free_request_ctx(struct raidx_request_ctx *ctx)
{
    if (ctx == NULL) {
        return;
    }

    if (ctx->stripes != NULL) {
        for (uint32_t stripe_idx = 0; stripe_idx < ctx->stripe_count; ++stripe_idx) {
            struct raidx_write_stripe *stripe = &ctx->stripes[stripe_idx];

            if (stripe->chunk_buffers == NULL) {
                continue;
            }

            for (uint8_t chunk_idx = 0; chunk_idx < ctx->raid_io->raid_bdev->num_base_rpcs; ++chunk_idx) {
                if (stripe->chunk_buffers[chunk_idx] != NULL) {
                    spdk_dma_free(stripe->chunk_buffers[chunk_idx]);
                }
            }

            free(stripe->chunk_buffers);
        }

        free(ctx->stripes);
    }

    free(ctx);
}

static void
raidx_complete_request_message(void *ctx)
{
    struct send_wr_wrapper *send_wrapper = static_cast<struct send_wr_wrapper *>(ctx);
    struct raidx_request_ctx *request_ctx = static_cast<struct raidx_request_ctx *>(send_wrapper->ctx);

    assert(request_ctx->pending > 0);
    request_ctx->pending--;

    if (request_ctx->pending == 0) {
        raid_bdev_io_complete_part(request_ctx->raid_io, request_ctx->total_blocks, SPDK_BDEV_IO_STATUS_SUCCESS);
        raidx_free_request_ctx(request_ctx);
    }
}

static int
raidx_dispatch_to_rpc_buffer(struct raid_bdev_io *raid_io, uint8_t chunk_idx, uint64_t base_offset_blocks,
                             uint64_t num_blocks, uint8_t cs_type, const struct iovec *iov, int iovcnt,
                             spdk_msg_fn cb, void *cb_ctx)
{
    struct raid_bdev *raid_bdev = raid_io->raid_bdev;
    struct raid_bdev_io_channel *raid_ch = raid_io->raid_ch;
    struct spdk_mem_map *mem_map = raid_ch->qp_group->qps[chunk_idx]->mem_map;
    struct send_wr_wrapper *send_wrapper = raid_get_send_wrapper(raid_io, chunk_idx);
    struct ibv_send_wr *send_wr;
    struct ibv_sge *sge;
    struct cs_message_t *cs_msg;
    struct ibv_mr *mr;

    assert(send_wrapper != NULL);

    send_wr = &send_wrapper->send_wr;
    sge = send_wr->sg_list;
    cs_msg = (struct cs_message_t *)sge->addr;

    cs_msg_init(cs_msg);
    cs_msg->type = cs_type;
    cs_msg->offset = base_offset_blocks;
    cs_msg->length = num_blocks;
    cs_msg->last_index = raid_bdev->num_base_rpcs;
    cs_msg->num_sge[0] = (uint8_t)iovcnt;
    cs_msg->num_sge[1] = 0;

    for (uint8_t i = 0; i < cs_msg->num_sge[0]; ++i) {
        cs_msg->sgl[i].addr = (uint64_t)iov[i].iov_base;
        cs_msg->sgl[i].len = (uint32_t)iov[i].iov_len;
        mr = (struct ibv_mr *)spdk_mem_map_translate(mem_map, cs_msg->sgl[i].addr, NULL);
        cs_msg->sgl[i].rkey = mr->rkey;
    }

    sge->length = sizeof(struct cs_message_t) - sizeof(struct sg_entry) * (32 - cs_msg->num_sge[0]);
    send_wr->imm_data = kReqTypeRW;
    send_wrapper->rtn_cnt = return_count(cs_msg);
    send_wrapper->cb = cb;
    send_wrapper->ctx = cb_ctx;
    send_wrapper->num_blocks = 0;

    return raid_dispatch_to_rpc(send_wrapper);
}

static int
raidx_dispatch_to_rpc_read(struct raidx_request_ctx *request_ctx, uint8_t chunk_idx,
                           uint64_t base_offset_blocks, uint64_t num_blocks,
                           const struct iovec *iov, int total_iovcnt,
                           uint64_t iov_offset, uint64_t chunk_len)
{
    struct raid_bdev_io *raid_io = request_ctx->raid_io;
    struct raid_bdev *raid_bdev = raid_io->raid_bdev;
    struct raid_bdev_io_channel *raid_ch = raid_io->raid_ch;
    struct spdk_mem_map *mem_map = raid_ch->qp_group->qps[chunk_idx]->mem_map;
    struct send_wr_wrapper *send_wrapper = raid_get_send_wrapper(raid_io, chunk_idx);
    struct ibv_send_wr *send_wr;
    struct ibv_sge *sge;
    struct cs_message_t *cs_msg;
    struct ibv_mr *mr;
    int chunk_iovcnt;

    assert(send_wrapper != NULL);

    send_wr = &send_wrapper->send_wr;
    sge = send_wr->sg_list;
    cs_msg = (struct cs_message_t *)sge->addr;

    cs_msg_init(cs_msg);
    chunk_iovcnt = raidx_map_iov(cs_msg->sgl, iov, total_iovcnt, iov_offset, chunk_len);
    if (chunk_iovcnt < 0) {
        return -EINVAL;
    }

    cs_msg->type = CS_RD;
    cs_msg->offset = base_offset_blocks;
    cs_msg->length = num_blocks;
    cs_msg->last_index = raid_bdev->num_base_rpcs;
    cs_msg->num_sge[0] = (uint8_t)chunk_iovcnt;
    cs_msg->num_sge[1] = 0;

    for (uint8_t i = 0; i < cs_msg->num_sge[0]; ++i) {
        mr = (struct ibv_mr *)spdk_mem_map_translate(mem_map, cs_msg->sgl[i].addr, NULL);
        cs_msg->sgl[i].rkey = mr->rkey;
    }

    sge->length = sizeof(struct cs_message_t) - sizeof(struct sg_entry) * (32 - cs_msg->num_sge[0]);
    send_wr->imm_data = kReqTypeRW;
    send_wrapper->rtn_cnt = return_count(cs_msg);
    send_wrapper->cb = raidx_complete_request_message;
    send_wrapper->ctx = request_ctx;
    send_wrapper->num_blocks = 0;

    return raid_dispatch_to_rpc(send_wrapper);
}

static int
raidx_dispatch_to_rpc_recon(struct raidx_request_ctx *request_ctx, uint8_t chunk_idx,
                            uint8_t data_index, uint8_t recon_coeff,
                            uint64_t recon_offset_blocks, uint64_t recon_num_blocks,
                            int8_t next_index, uint8_t fwd_num,
                            const struct iovec *target_iov, int target_iovcnt,
                            uint64_t target_iov_offset, uint64_t target_iov_len)
{
    struct raid_bdev_io *raid_io = request_ctx->raid_io;
    struct raid_bdev *raid_bdev = raid_io->raid_bdev;
    struct raid_bdev_io_channel *raid_ch = raid_io->raid_ch;
    struct spdk_mem_map *mem_map = raid_ch->qp_group->qps[chunk_idx]->mem_map;
    struct send_wr_wrapper *send_wrapper = raid_get_send_wrapper(raid_io, chunk_idx);
    struct ibv_send_wr *send_wr;
    struct ibv_sge *sge;
    struct cs_message_t *cs_msg;
    struct ibv_mr *mr;
    int target_chunk_iovcnt = 0;

    assert(send_wrapper != NULL);

    send_wr = &send_wrapper->send_wr;
    sge = send_wr->sg_list;
    cs_msg = (struct cs_message_t *)sge->addr;

    cs_msg_init(cs_msg);
    if (target_iov != NULL && target_iov_len > 0) {
        target_chunk_iovcnt = raidx_map_iov(cs_msg->sgl, target_iov, target_iovcnt,
                                            target_iov_offset, target_iov_len);
        if (target_chunk_iovcnt < 0) {
            return -EINVAL;
        }
    }

    cs_msg->type = RECON_NRT;
    cs_msg->data_index = data_index;
    cs_msg->recon_coeff = recon_coeff;
    cs_msg->offset = 0;
    cs_msg->length = 0;
    cs_msg->fwd_offset = recon_offset_blocks;
    cs_msg->fwd_length = recon_num_blocks;
    cs_msg->next_index = next_index;
    cs_msg->fwd_num = fwd_num;
    cs_msg->last_index = raid_bdev->num_base_rpcs;
    cs_msg->num_sge[0] = 0;
    cs_msg->num_sge[1] = (uint8_t)target_chunk_iovcnt;

    for (uint8_t i = 0; i < cs_msg->num_sge[1]; ++i) {
        mr = (struct ibv_mr *)spdk_mem_map_translate(mem_map, cs_msg->sgl[i].addr, NULL);
        cs_msg->sgl[i].rkey = mr->rkey;
    }

    sge->length = sizeof(struct cs_message_t) - sizeof(struct sg_entry) *
                  (32 - cs_msg->num_sge[0] - cs_msg->num_sge[1]);
    send_wr->imm_data = kReqTypeReconRead;
    send_wrapper->rtn_cnt = return_count(cs_msg);
    send_wrapper->cb = raidx_complete_request_message;
    send_wrapper->ctx = request_ctx;
    send_wrapper->num_blocks = 0;

    return raid_dispatch_to_rpc(send_wrapper);
}

static int
raidx_dispatch_to_rpc_recon_write(struct raidx_request_ctx *request_ctx, uint8_t chunk_idx,
                                  uint8_t data_index, uint8_t recon_coeff,
                                  uint64_t recon_offset_blocks, uint64_t recon_num_blocks,
                                  uint64_t parity_offset_blocks, uint64_t parity_num_blocks,
                                  int8_t next_index, uint8_t fwd_num,
                                  const struct iovec *iov, int total_iovcnt,
                                  uint64_t iov_offset, uint64_t iov_len,
                                  const int8_t *target_index, uint8_t target_num,
                                  const uint8_t *parity_row, uint8_t parity_num,
                                  const uint8_t *coeff, uint8_t coeff_num)
{
    struct raid_bdev_io *raid_io = request_ctx->raid_io;
    struct raid_bdev *raid_bdev = raid_io->raid_bdev;
    struct raid_bdev_io_channel *raid_ch = raid_io->raid_ch;
    struct spdk_mem_map *mem_map = raid_ch->qp_group->qps[chunk_idx]->mem_map;
    struct send_wr_wrapper *send_wrapper = raid_get_send_wrapper(raid_io, chunk_idx);
    struct ibv_send_wr *send_wr;
    struct ibv_sge *sge;
    struct cs_message_t *cs_msg;
    struct ibv_mr *mr;
    int chunk_iovcnt = 0;

    assert(send_wrapper != NULL);

    send_wr = &send_wrapper->send_wr;
    sge = send_wr->sg_list;
    cs_msg = (struct cs_message_t *)sge->addr;

    cs_msg_init(cs_msg);
    if (iov != NULL && iov_len > 0) {
        int ret = raidx_copy_payload_to_send_wrapper(send_wrapper, iov, total_iovcnt,
                                                     iov_offset, iov_len);
        if (ret != 0) {
            return ret;
        }
        chunk_iovcnt = 1;
    }

    cs_msg->type = RECON_RW_DIFF;
    cs_msg->data_index = data_index;
    cs_msg->recon_coeff = recon_coeff;
    cs_msg->offset = recon_offset_blocks;
    cs_msg->length = recon_num_blocks;
    cs_msg->fwd_offset = recon_offset_blocks;
    cs_msg->fwd_length = recon_num_blocks;
    cs_msg->y_offset = parity_offset_blocks;
    cs_msg->y_length = parity_num_blocks;
    cs_msg->next_index = next_index;
    cs_msg->recon_next_index = next_index;
    cs_msg->fwd_num = fwd_num;
    cs_msg->target_num = target_num;
    cs_msg->parity_num = parity_num;
    cs_msg->coeff_num = coeff_num;
    cs_msg->last_index = raid_bdev->num_base_rpcs;
    cs_msg->num_sge[0] = (uint8_t)chunk_iovcnt;
    cs_msg->num_sge[1] = 0;

    for (uint8_t i = 0; i < target_num; ++i) {
        cs_msg->target_index[i] = target_index[i];
    }

    for (uint8_t i = 0; i < parity_num; ++i) {
        cs_msg->parity_row[i] = parity_row[i];
    }

    for (uint8_t i = 0; i < coeff_num; ++i) {
        cs_msg->coeff[i] = coeff[i];
    }

    if (chunk_iovcnt > 0) {
        cs_msg->sgl[0].addr = (uint64_t)send_wrapper->buf;
        cs_msg->sgl[0].len = (uint32_t)iov_len;
        mr = (struct ibv_mr *)spdk_mem_map_translate(mem_map, cs_msg->sgl[0].addr, NULL);
        cs_msg->sgl[0].rkey = mr->rkey;
    }

    sge->length = sizeof(struct cs_message_t) - sizeof(struct sg_entry) *
                  (32 - cs_msg->num_sge[0] - cs_msg->num_sge[1]);
    send_wr->imm_data = kReqTypeReconRead;
    send_wrapper->rtn_cnt = return_count(cs_msg);
    send_wrapper->cb = raidx_complete_request_message;
    send_wrapper->ctx = request_ctx;
    send_wrapper->num_blocks = 0;

    return raid_dispatch_to_rpc(send_wrapper);
}

static int
raidx_dispatch_to_rpc_partial(struct raidx_request_ctx *request_ctx, uint8_t chunk_idx,
                              uint64_t base_offset_blocks, uint64_t num_blocks,
                              uint64_t fwd_offset_blocks, uint64_t fwd_num_blocks,
                              uint8_t msg_type, uint8_t cs_type, uint8_t data_index,
                              uint8_t fwd_num, const struct iovec *iov, int total_iovcnt,
                              uint64_t iov_offset, uint64_t iov_len,
                              const int8_t *target_index, uint8_t target_num,
                              const uint8_t *parity_row, uint8_t parity_num,
                              const uint8_t *coeff, uint8_t coeff_num)
{
    struct raid_bdev_io *raid_io = request_ctx->raid_io;
    struct raid_bdev *raid_bdev = raid_io->raid_bdev;
    struct raid_bdev_io_channel *raid_ch = raid_io->raid_ch;
    struct spdk_mem_map *mem_map = raid_ch->qp_group->qps[chunk_idx]->mem_map;
    struct send_wr_wrapper *send_wrapper = raid_get_send_wrapper(raid_io, chunk_idx);
    struct ibv_send_wr *send_wr;
    struct ibv_sge *sge;
    struct cs_message_t *cs_msg;
    struct ibv_mr *mr;
    int chunk_iovcnt = 0;

    assert(send_wrapper != NULL);

    send_wr = &send_wrapper->send_wr;
    sge = send_wr->sg_list;
    cs_msg = (struct cs_message_t *)sge->addr;

    cs_msg_init(cs_msg);
    if (iov != NULL && iov_len > 0) {
        int ret = raidx_copy_payload_to_send_wrapper(send_wrapper, iov, total_iovcnt,
                                                     iov_offset, iov_len);
        if (ret != 0) {
            return ret;
        }
        chunk_iovcnt = 1;
    }

    cs_msg->type = cs_type;
    cs_msg->data_index = data_index;
    cs_msg->offset = base_offset_blocks;
    cs_msg->length = num_blocks;
    cs_msg->fwd_offset = fwd_offset_blocks;
    cs_msg->fwd_length = fwd_num_blocks;
    cs_msg->fwd_num = fwd_num;
    cs_msg->target_num = target_num;
    cs_msg->parity_num = parity_num;
    cs_msg->coeff_num = coeff_num;
    cs_msg->last_index = raid_bdev->num_base_rpcs;
    cs_msg->num_sge[0] = (uint8_t)chunk_iovcnt;
    cs_msg->num_sge[1] = 0;

    for (uint8_t i = 0; i < target_num; ++i) {
        cs_msg->target_index[i] = target_index[i];
    }

    for (uint8_t i = 0; i < parity_num; ++i) {
        cs_msg->parity_row[i] = parity_row[i];
    }

    for (uint8_t i = 0; i < coeff_num; ++i) {
        cs_msg->coeff[i] = coeff[i];
    }

    if (chunk_iovcnt > 0) {
        cs_msg->sgl[0].addr = (uint64_t)send_wrapper->buf;
        cs_msg->sgl[0].len = (uint32_t)iov_len;
        mr = (struct ibv_mr *)spdk_mem_map_translate(mem_map, cs_msg->sgl[0].addr, NULL);
        cs_msg->sgl[0].rkey = mr->rkey;
    }

    sge->length = sizeof(struct cs_message_t) - sizeof(struct sg_entry) *
                  (32 - cs_msg->num_sge[0] - cs_msg->num_sge[1]);
    send_wr->imm_data = msg_type;
    send_wrapper->rtn_cnt = return_count(cs_msg);
    send_wrapper->cb = raidx_complete_request_message;
    send_wrapper->ctx = request_ctx;
    send_wrapper->num_blocks = 0;

    return raid_dispatch_to_rpc(send_wrapper);
}

static int
raidx_dispatch_read_chunk(struct raidx_request_ctx *request_ctx, uint64_t stripe_index,
                          const struct raidx_partial_chunk *chunk, const struct iovec *iov, int iovcnt)
{
    struct raid_bdev *raid_bdev = request_ctx->raid_io->raid_bdev;
    uint64_t base_offset_blocks = (stripe_index << raid_bdev->strip_size_shift) + chunk->req_offset;
    uint64_t chunk_len = chunk->req_blocks << raid_bdev->blocklen_shift;
    int ret;

    ret = raidx_dispatch_to_rpc_read(request_ctx, chunk->chunk_idx, base_offset_blocks,
                                     chunk->req_blocks, iov, iovcnt, chunk->iov_offset, chunk_len);
    if (ret == 0) {
        request_ctx->pending++;
    }

    return ret;
}

static int
raidx_dispatch_reconstructed_read(struct raidx_request_ctx *request_ctx, uint64_t stripe_index,
                                  uint64_t stripe_offset, uint64_t blocks_this_stripe,
                                  uint64_t iov_offset, const struct raidx_stripe_state *stripe_state)
{
    struct raid_bdev_io *raid_io = request_ctx->raid_io;
    struct raid_bdev *raid_bdev = raid_io->raid_bdev;
    struct spdk_bdev_io *bdev_io = spdk_bdev_io_from_ctx(raid_io);
    uint64_t stripe_end = stripe_offset + blocks_this_stripe;
    uint64_t chunk_blocks = raid_bdev->strip_size;
    uint8_t degraded_total = stripe_state->degraded_data_count + stripe_state->degraded_parity_count;
    std::vector<struct raidx_partial_chunk> direct_chunks;
    std::vector<struct raidx_partial_chunk> missing_chunks;
    std::vector<struct raidx_recon_plan> recon_plans;

    if (degraded_total > raid_bdev->num_parities) {
        return -ENOTSUP;
    }

    for (uint8_t chunk_idx = 0, data_index = 0; chunk_idx < raid_bdev->num_base_rpcs; ++chunk_idx) {
        uint64_t chunk_from;
        uint64_t chunk_to;
        uint64_t overlap_from;
        uint64_t overlap_to;

        if (raidx_is_parity_slot(stripe_index, raid_bdev->num_base_rpcs,
                                 raid_bdev->num_parities, chunk_idx)) {
            continue;
        }

        chunk_from = data_index * chunk_blocks;
        chunk_to = chunk_from + chunk_blocks;
        overlap_from = stripe_offset > chunk_from ? stripe_offset : chunk_from;
        overlap_to = stripe_end < chunk_to ? stripe_end : chunk_to;

        if (overlap_from < overlap_to) {
            struct raidx_partial_chunk chunk = {};

            chunk.chunk_idx = chunk_idx;
            chunk.data_index = data_index;
            chunk.req_offset = overlap_from - chunk_from;
            chunk.req_blocks = overlap_to - overlap_from;
            chunk.iov_offset = iov_offset + ((overlap_from - stripe_offset) << raid_bdev->blocklen_shift);

            if (raid_bdev->base_rpc_info[chunk_idx].degraded) {
                missing_chunks.push_back(chunk);
            } else {
                direct_chunks.push_back(chunk);
            }
        }

        data_index++;
    }

    for (const auto &missing_chunk : missing_chunks) {
        struct raidx_recon_plan plan;
        int ret = raidx_build_recon_plan(raid_bdev, stripe_index, &missing_chunk, &plan);
        if (ret != 0) {
            return ret;
        }
        recon_plans.push_back(plan);
    }

    for (const auto &chunk : direct_chunks) {
        int ret = raidx_dispatch_read_chunk(request_ctx, stripe_index, &chunk,
                                            bdev_io->u.bdev.iovs, bdev_io->u.bdev.iovcnt);
        if (ret != 0) {
            return ret;
        }
    }

    for (const auto &plan : recon_plans) {
        const struct raidx_partial_chunk &missing_chunk = plan.missing_chunk;
        uint8_t root_slot = plan.sources[0].slot;
        uint8_t source_count = (uint8_t)plan.sources.size();
        uint64_t recon_base_offset_blocks = (stripe_index << raid_bdev->strip_size_shift) + missing_chunk.req_offset;
        uint64_t recon_len = missing_chunk.req_blocks << raid_bdev->blocklen_shift;

        if (raidx_trace_degraded_enabled()) {
            SPDK_NOTICELOG("RAIDX_RECON_READ stripe=%llu missing_slot=%u data_index=%u "
                           "offset_blocks=%llu blocks=%llu root_slot=%u sources=%u\n",
                           (unsigned long long)stripe_index, missing_chunk.chunk_idx,
                           missing_chunk.data_index,
                           (unsigned long long)recon_base_offset_blocks,
                           (unsigned long long)missing_chunk.req_blocks,
                           root_slot, source_count);
        }

        for (const auto &source : plan.sources) {
            bool is_root = source.slot == root_slot;
            if (raidx_trace_degraded_enabled()) {
                SPDK_NOTICELOG("RAIDX_RECON_SOURCE stripe=%llu missing_slot=%u source_slot=%u "
                               "coeff=%u root=%u\n",
                               (unsigned long long)stripe_index, missing_chunk.chunk_idx,
                               source.slot, source.coeff, is_root ? 1 : 0);
            }
            int ret = raidx_dispatch_to_rpc_recon(request_ctx, source.slot,
                                                  missing_chunk.data_index, source.coeff,
                                                  recon_base_offset_blocks, missing_chunk.req_blocks,
                                                  is_root ? -1 : (int8_t)root_slot,
                                                  is_root ? (uint8_t)(source_count - 1) : 0,
                                                  is_root ? bdev_io->u.bdev.iovs : NULL,
                                                  is_root ? bdev_io->u.bdev.iovcnt : 0,
                                                  missing_chunk.iov_offset,
                                                  is_root ? recon_len : 0);
            if (ret != 0) {
                return ret;
            }

            request_ctx->pending++;
        }
    }

    return 0;
}

static int
raidx_submit_read(struct raid_bdev_io *raid_io, struct raidx_info *rxinfo,
                  uint64_t stripe_index, uint64_t stripe_offset, uint64_t num_blocks)
{
    struct raidx_request_ctx *request_ctx;
    uint64_t remaining = num_blocks;
    uint64_t iov_offset = 0;

    request_ctx = (struct raidx_request_ctx *)calloc(1, sizeof(*request_ctx));
    if (request_ctx == NULL) {
        return -ENOMEM;
    }

    request_ctx->raid_io = raid_io;
    request_ctx->total_blocks = num_blocks;

    while (remaining > 0) {
        struct raid_bdev *raid_bdev = raid_io->raid_bdev;
        struct raidx_stripe_state stripe_state;
        uint64_t blocks_this_stripe = spdk_min(remaining, rxinfo->stripe_blocks - stripe_offset);
        int ret;

        raidx_collect_stripe_state(raid_bdev, stripe_index, &stripe_state);
        ret = raidx_dispatch_reconstructed_read(request_ctx, stripe_index, stripe_offset,
                                                blocks_this_stripe, iov_offset, &stripe_state);
        if (ret != 0) {
            if (request_ctx->pending == 0) {
                raidx_free_request_ctx(request_ctx);
            } else {
                SPDK_ERRLOG("raidx read dispatch failed after partial submission\n");
                assert(false);
            }
            return ret;
        }

        remaining -= blocks_this_stripe;
        iov_offset += blocks_this_stripe << raid_bdev->blocklen_shift;
        stripe_index++;
        stripe_offset = 0;
    }

    return 0;
}

static int
raidx_alloc_write_stripe(struct raid_bdev *raid_bdev, struct raidx_write_stripe *stripe)
{
    size_t chunk_len = (size_t)raid_bdev->strip_size << raid_bdev->blocklen_shift;

    stripe->chunk_buffers = (void **)calloc(raid_bdev->num_base_rpcs, sizeof(void *));
    if (stripe->chunk_buffers == NULL) {
        return -ENOMEM;
    }

    for (uint8_t chunk_idx = 0; chunk_idx < raid_bdev->num_base_rpcs; ++chunk_idx) {
        stripe->chunk_buffers[chunk_idx] = spdk_dma_malloc(chunk_len, 32, NULL);
        if (stripe->chunk_buffers[chunk_idx] == NULL) {
            for (uint8_t free_idx = 0; free_idx < chunk_idx; ++free_idx) {
                spdk_dma_free(stripe->chunk_buffers[free_idx]);
            }
            free(stripe->chunk_buffers);
            stripe->chunk_buffers = NULL;
            return -ENOMEM;
        }
    }

    return 0;
}

static int
raidx_prepare_full_stripe(struct raidx_request_ctx *request_ctx, struct raidx_info *rxinfo,
                          uint32_t stripe_array_idx, uint64_t stripe_index, uint64_t iov_src_offset)
{
    struct raid_bdev_io *raid_io = request_ctx->raid_io;
    struct raid_bdev *raid_bdev = raid_io->raid_bdev;
    struct spdk_bdev_io *bdev_io = spdk_bdev_io_from_ctx(raid_io);
    struct raidx_write_stripe *stripe = &request_ctx->stripes[stripe_array_idx];
    size_t chunk_len = (size_t)raid_bdev->strip_size << raid_bdev->blocklen_shift;
    uint8_t total_chunks = raid_bdev->num_base_rpcs;
    uint8_t num_data_chunks = raid_bdev->num_data_chunks;
    uint8_t num_parities = raid_bdev->num_parities;
    int ret;

    ret = raidx_alloc_write_stripe(raid_bdev, stripe);
    if (ret != 0) {
        return ret;
    }

    std::vector<unsigned char *> data_ptrs(num_data_chunks);
    std::vector<unsigned char *> parity_ptrs(num_parities);

    for (uint8_t data_idx = 0; data_idx < num_data_chunks; ++data_idx) {
        int8_t slot = raidx_data_slot(stripe_index, total_chunks, num_parities, data_idx);
        struct iovec dst_iov;

        assert(slot >= 0);
        if (raidx_trace_layout_enabled()) {
            SPDK_NOTICELOG("RAIDX_LAYOUT stripe=%llu slot=%d role=data data_index=%u\n",
                           (unsigned long long)stripe_index, slot, data_idx);
        }
        dst_iov.iov_base = stripe->chunk_buffers[slot];
        dst_iov.iov_len = chunk_len;
        raid_memcpy_iovs(&dst_iov, 1, 0,
                         bdev_io->u.bdev.iovs, bdev_io->u.bdev.iovcnt,
                         iov_src_offset + (data_idx * chunk_len), chunk_len);
        data_ptrs[data_idx] = (unsigned char *)stripe->chunk_buffers[slot];
    }

    for (uint8_t row = 0; row < num_parities; ++row) {
        uint8_t slot = raidx_parity_slot(stripe_index, total_chunks, row);
        if (raidx_trace_layout_enabled()) {
            SPDK_NOTICELOG("RAIDX_LAYOUT stripe=%llu slot=%u role=parity parity_row=%u\n",
                           (unsigned long long)stripe_index, slot, row);
        }
        memset(stripe->chunk_buffers[slot], 0, chunk_len);
        parity_ptrs[row] = (unsigned char *)stripe->chunk_buffers[slot];
    }

    raidx_ec_encode_full_stripe(&rxinfo->ec, chunk_len, data_ptrs.data(), parity_ptrs.data());
    return 0;
}

static int
raidx_dispatch_full_stripe(struct raidx_request_ctx *request_ctx, uint32_t stripe_array_idx, uint64_t stripe_index,
                           const struct raidx_stripe_state *stripe_state)
{
    struct raid_bdev_io *raid_io = request_ctx->raid_io;
    struct raid_bdev *raid_bdev = raid_io->raid_bdev;
    struct raidx_write_stripe *stripe = &request_ctx->stripes[stripe_array_idx];

    for (uint8_t chunk_idx = 0; chunk_idx < raid_bdev->num_base_rpcs; ++chunk_idx) {
        struct iovec chunk_iov;
        int ret;

        if (raid_bdev->base_rpc_info[chunk_idx].degraded) {
            if (raidx_trace_degraded_enabled()) {
                SPDK_NOTICELOG("RAIDX_DEGRADED_SKIP stripe=%llu slot=%u op=full_write\n",
                               (unsigned long long)stripe_index, chunk_idx);
            }
            continue;
        }

        chunk_iov.iov_base = stripe->chunk_buffers[chunk_idx];
        chunk_iov.iov_len = (size_t)raid_bdev->strip_size << raid_bdev->blocklen_shift;
        ret = raidx_dispatch_to_rpc_buffer(raid_io, chunk_idx,
                                           stripe_index << raid_bdev->strip_size_shift,
                                           raid_bdev->strip_size, CS_WT, &chunk_iov, 1,
                                           raidx_complete_request_message, request_ctx);
        if (ret != 0) {
            return ret;
        }

        request_ctx->pending++;
    }

    return 0;
}

static int
raidx_dispatch_partial_write(struct raidx_request_ctx *request_ctx, uint64_t stripe_index,
                             uint64_t stripe_offset, uint64_t blocks_this_stripe, uint64_t iov_offset,
                             const struct raidx_stripe_state *stripe_state)
{
    struct raid_bdev_io *raid_io = request_ctx->raid_io;
    struct raid_bdev *raid_bdev = raid_io->raid_bdev;
    struct spdk_bdev_io *bdev_io = spdk_bdev_io_from_ctx(raid_io);
    uint64_t stripe_end = stripe_offset + blocks_this_stripe;
    uint64_t chunk_blocks = raid_bdev->strip_size;
    uint64_t parity_base_offset = stripe_index << raid_bdev->strip_size_shift;
    std::vector<struct raidx_partial_chunk> touched_chunks;
    std::vector<struct raidx_recon_plan> recon_write_plans;
    int8_t target_index[kRaidxMaxTargets];
    uint8_t parity_row[kRaidxMaxParityRows];
    int ret;

    for (uint8_t i = 0; i < stripe_state->surviving_parity_count; ++i) {
        target_index[i] = stripe_state->surviving_parity_slots[i];
        parity_row[i] = stripe_state->surviving_parity_rows[i];
    }

    for (uint8_t chunk_idx = 0, data_index = 0; chunk_idx < raid_bdev->num_base_rpcs; ++chunk_idx) {
        uint64_t chunk_from;
        uint64_t chunk_to;
        uint64_t overlap_from;
        uint64_t overlap_to;

        if (raidx_is_parity_slot(stripe_index, raid_bdev->num_base_rpcs,
                                 raid_bdev->num_parities, chunk_idx)) {
            continue;
        }

        chunk_from = data_index * chunk_blocks;
        chunk_to = chunk_from + chunk_blocks;
        overlap_from = stripe_offset > chunk_from ? stripe_offset : chunk_from;
        overlap_to = stripe_end < chunk_to ? stripe_end : chunk_to;

        if (overlap_from < overlap_to) {
            struct raidx_partial_chunk chunk = {};
            chunk.chunk_idx = chunk_idx;
            chunk.data_index = data_index;
            chunk.req_offset = overlap_from - chunk_from;
            chunk.req_blocks = overlap_to - overlap_from;
            chunk.iov_offset = iov_offset + ((overlap_from - stripe_offset) << raid_bdev->blocklen_shift);
            touched_chunks.push_back(chunk);
        }

        data_index++;
    }

    if (touched_chunks.empty()) {
        return 0;
    }

    if (stripe_state->degraded_data_count == 0 && stripe_state->degraded_parity_count == 0) {
        for (const auto &chunk : touched_chunks) {
            uint64_t chunk_len = chunk.req_blocks << raid_bdev->blocklen_shift;
            uint64_t base_offset_blocks = (stripe_index << raid_bdev->strip_size_shift) + chunk.req_offset;
            uint64_t parity_offset_blocks = parity_base_offset + chunk.req_offset;
            uint8_t coeff[kRaidxMaxParityRows];

            for (uint8_t parity_idx = 0; parity_idx < stripe_state->surviving_parity_count; ++parity_idx) {
                uint8_t parity_chunk_idx = stripe_state->surviving_parity_slots[parity_idx];
                uint8_t single_row = stripe_state->surviving_parity_rows[parity_idx];

                coeff[parity_idx] = raidx_parity_coeff(single_row, chunk.data_index);

                ret = raidx_dispatch_to_rpc_partial(request_ctx, parity_chunk_idx,
                                                    parity_offset_blocks, chunk.req_blocks,
                                                    parity_offset_blocks, chunk.req_blocks,
                                                    kReqTypeParity, PR_DIFF, chunk.data_index,
                                                    1, NULL, 0, 0, 0,
                                                    NULL, 0, &single_row, 1,
                                                    NULL, 0);
                if (ret != 0) {
                    return ret;
                }

                request_ctx->pending++;
            }

            ret = raidx_dispatch_to_rpc_partial(request_ctx, chunk.chunk_idx,
                                                base_offset_blocks, chunk.req_blocks,
                                                parity_offset_blocks, chunk.req_blocks,
                                                kReqTypePartialWrite, FWD_RW_DIFF, chunk.data_index,
                                                0, bdev_io->u.bdev.iovs, bdev_io->u.bdev.iovcnt,
                                                chunk.iov_offset, chunk_len,
                                                target_index, stripe_state->surviving_parity_count,
                                                parity_row, stripe_state->surviving_parity_count,
                                                coeff, stripe_state->surviving_parity_count);
            if (ret != 0) {
                return ret;
            }

            request_ctx->pending++;
        }

        return 0;
    }

    for (const auto &chunk : touched_chunks) {
        if (raid_bdev->base_rpc_info[chunk.chunk_idx].degraded) {
            struct raidx_recon_plan plan;
            ret = raidx_build_recon_plan(raid_bdev, stripe_index, &chunk, &plan);
            if (ret != 0) {
                return ret;
            }
            recon_write_plans.push_back(plan);
        }
    }

    for (uint8_t parity_idx = 0; parity_idx < stripe_state->surviving_parity_count; ++parity_idx) {
        uint8_t parity_chunk_idx = stripe_state->surviving_parity_slots[parity_idx];
        uint8_t single_row = stripe_state->surviving_parity_rows[parity_idx];

        if (raidx_trace_degraded_enabled()) {
            SPDK_NOTICELOG("RAIDX_PARITY_TARGET stripe=%llu slot=%u parity_row=%u op=partial_write\n",
                           (unsigned long long)stripe_index, parity_chunk_idx, single_row);
        }

        ret = raidx_dispatch_to_rpc_partial(request_ctx, parity_chunk_idx,
                                            parity_base_offset, raid_bdev->strip_size,
                                            parity_base_offset, raid_bdev->strip_size,
                                            kReqTypeParity, PR_DIFF, 0,
                                            (uint8_t)touched_chunks.size(),
                                            NULL, 0, 0, 0,
                                            NULL, 0, &single_row, 1,
                                            NULL, 0);
        if (ret != 0) {
            return ret;
        }

        request_ctx->pending++;
    }

    for (const auto &chunk : touched_chunks) {
        uint64_t chunk_len = chunk.req_blocks << raid_bdev->blocklen_shift;
        uint64_t base_offset_blocks = (stripe_index << raid_bdev->strip_size_shift) + chunk.req_offset;
        uint8_t coeff[kRaidxMaxParityRows];

        for (uint8_t parity_idx = 0; parity_idx < stripe_state->surviving_parity_count; ++parity_idx) {
            coeff[parity_idx] = raidx_parity_coeff(stripe_state->surviving_parity_rows[parity_idx], chunk.data_index);
        }

        if (raid_bdev->base_rpc_info[chunk.chunk_idx].degraded) {
            const struct raidx_recon_plan *plan = NULL;

            for (const auto &candidate : recon_write_plans) {
                if (candidate.missing_chunk.chunk_idx == chunk.chunk_idx &&
                    candidate.missing_chunk.req_offset == chunk.req_offset &&
                    candidate.missing_chunk.req_blocks == chunk.req_blocks) {
                    plan = &candidate;
                    break;
                }
            }

            if (plan == NULL) {
                return -ENOTSUP;
            }

            uint8_t root_slot = plan->sources[0].slot;
            uint8_t source_count = (uint8_t)plan->sources.size();
            if (raidx_trace_degraded_enabled()) {
                SPDK_NOTICELOG("RAIDX_RECON_WRITE stripe=%llu missing_slot=%u data_index=%u "
                               "offset_blocks=%llu blocks=%llu root_slot=%u sources=%u targets=%u\n",
                               (unsigned long long)stripe_index, chunk.chunk_idx,
                               chunk.data_index, (unsigned long long)base_offset_blocks,
                               (unsigned long long)chunk.req_blocks, root_slot, source_count,
                               stripe_state->surviving_parity_count);
            }
            for (const auto &source : plan->sources) {
                bool is_root = source.slot == root_slot;
                if (raidx_trace_degraded_enabled()) {
                    SPDK_NOTICELOG("RAIDX_RECON_WRITE_SOURCE stripe=%llu missing_slot=%u "
                                   "source_slot=%u coeff=%u root=%u\n",
                                   (unsigned long long)stripe_index, chunk.chunk_idx,
                                   source.slot, source.coeff, is_root ? 1 : 0);
                }
                ret = raidx_dispatch_to_rpc_recon_write(request_ctx, source.slot,
                                                        chunk.data_index, source.coeff,
                                                        base_offset_blocks, chunk.req_blocks,
                                                        parity_base_offset, raid_bdev->strip_size,
                                                        is_root ? -1 : (int8_t)root_slot,
                                                        is_root ? (uint8_t)(source_count - 1) : 0,
                                                        is_root ? bdev_io->u.bdev.iovs : NULL,
                                                        is_root ? bdev_io->u.bdev.iovcnt : 0,
                                                        chunk.iov_offset,
                                                        is_root ? chunk_len : 0,
                                                        target_index, stripe_state->surviving_parity_count,
                                                        parity_row, stripe_state->surviving_parity_count,
                                                        coeff, stripe_state->surviving_parity_count);
                if (ret != 0) {
                    return ret;
                }

                request_ctx->pending++;
            }
            continue;
        }

        ret = raidx_dispatch_to_rpc_partial(request_ctx, chunk.chunk_idx,
                                            base_offset_blocks, chunk.req_blocks,
                                            parity_base_offset, raid_bdev->strip_size,
                                            kReqTypePartialWrite, FWD_RW_DIFF, chunk.data_index,
                                            0, bdev_io->u.bdev.iovs, bdev_io->u.bdev.iovcnt,
                                            chunk.iov_offset, chunk_len,
                                            target_index, stripe_state->surviving_parity_count,
                                            parity_row, stripe_state->surviving_parity_count,
                                            coeff, stripe_state->surviving_parity_count);
        if (ret != 0) {
            return ret;
        }

        request_ctx->pending++;
    }

    return 0;
}

static uint32_t
raidx_count_write_stripes(const struct raidx_info *rxinfo, uint64_t stripe_offset, uint64_t num_blocks)
{
    uint32_t count = 0;
    uint64_t remaining = num_blocks;

    while (remaining > 0) {
        uint64_t blocks_this_stripe = spdk_min(remaining, rxinfo->stripe_blocks - stripe_offset);
        remaining -= blocks_this_stripe;
        stripe_offset = 0;
        count++;
    }

    return count;
}

static int
raidx_submit_write(struct raid_bdev_io *raid_io, struct raidx_info *rxinfo,
                   uint64_t stripe_index, uint64_t stripe_offset, uint64_t num_blocks)
{
    struct raid_bdev *raid_bdev = raid_io->raid_bdev;
    struct raidx_request_ctx *request_ctx;
    uint64_t remaining = num_blocks;
    uint64_t iov_offset = 0;
    uint32_t stripe_array_idx = 0;

    request_ctx = (struct raidx_request_ctx *)calloc(1, sizeof(*request_ctx));
    if (request_ctx == NULL) {
        return -ENOMEM;
    }

    request_ctx->raid_io = raid_io;
    request_ctx->total_blocks = num_blocks;
    request_ctx->stripe_count = raidx_count_write_stripes(rxinfo, stripe_offset, num_blocks);
    request_ctx->stripes = (struct raidx_write_stripe *)calloc(request_ctx->stripe_count, sizeof(*request_ctx->stripes));
    if (request_ctx->stripes == NULL) {
        free(request_ctx);
        return -ENOMEM;
    }

    while (remaining > 0) {
        struct raidx_stripe_state stripe_state;
        uint64_t blocks_this_stripe = spdk_min(remaining, rxinfo->stripe_blocks - stripe_offset);
        bool is_full_stripe_write = stripe_offset == 0 && blocks_this_stripe == rxinfo->stripe_blocks;
        int ret;

        raidx_collect_stripe_state(raid_bdev, stripe_index, &stripe_state);
        if (!raidx_write_stripe_is_supported(raid_bdev, &stripe_state, is_full_stripe_write)) {
            if (request_ctx->pending == 0) {
                raidx_free_request_ctx(request_ctx);
            }
            return -ENOTSUP;
        }

        if (is_full_stripe_write) {
            ret = raidx_prepare_full_stripe(request_ctx, rxinfo, stripe_array_idx, stripe_index, iov_offset);
            if (ret != 0) {
                if (request_ctx->pending == 0) {
                    raidx_free_request_ctx(request_ctx);
                }
                return ret;
            }

            ret = raidx_dispatch_full_stripe(request_ctx, stripe_array_idx, stripe_index, &stripe_state);
        } else {
            ret = raidx_dispatch_partial_write(request_ctx, stripe_index, stripe_offset,
                                               blocks_this_stripe, iov_offset, &stripe_state);
        }

        if (ret != 0) {
            if (request_ctx->pending == 0) {
                raidx_free_request_ctx(request_ctx);
            } else {
                SPDK_ERRLOG("raidx write dispatch failed after partial submission\n");
                assert(false);
            }
            return ret;
        }

        remaining -= blocks_this_stripe;
        iov_offset += blocks_this_stripe << raid_bdev->blocklen_shift;
        stripe_index++;
        stripe_offset = 0;
        stripe_array_idx++;
    }

    return 0;
}

static void
raidx_submit_rw_request(struct raid_bdev_io *raid_io)
{
    struct raid_bdev *raid_bdev = raid_io->raid_bdev;
    struct spdk_bdev_io *bdev_io = spdk_bdev_io_from_ctx(raid_io);
    struct raidx_info *rxinfo = static_cast<struct raidx_info *>(raid_bdev->module_private);
    uint64_t offset_blocks = bdev_io->u.bdev.offset_blocks;
    uint64_t num_blocks = bdev_io->u.bdev.num_blocks;
    uint64_t stripe_index = offset_blocks / rxinfo->stripe_blocks;
    uint64_t stripe_offset = offset_blocks % rxinfo->stripe_blocks;
    int ret;

    if (bdev_io->type == SPDK_BDEV_IO_TYPE_READ) {
        raid_io->base_bdev_io_remaining = num_blocks;
        ret = raidx_submit_read(raid_io, rxinfo, stripe_index, stripe_offset, num_blocks);
        if (ret != 0) {
            if (ret == -ENOTSUP) {
                SPDK_ERRLOG("raidx degraded-data read could not build a valid GF reconstruction plan\n");
            }
            raid_bdev_io_complete(raid_io, ret == -ENOMEM ? SPDK_BDEV_IO_STATUS_NOMEM : SPDK_BDEV_IO_STATUS_FAILED);
        }
        return;
    }

    raid_io->base_bdev_io_remaining = num_blocks;
    ret = raidx_submit_write(raid_io, rxinfo, stripe_index, stripe_offset, num_blocks);
    if (ret != 0) {
        if (ret == -ENOTSUP) {
            SPDK_ERRLOG("raidx degraded-data write could not build a valid raidx update plan\n");
        }
        raid_bdev_io_complete(raid_io, ret == -ENOMEM ? SPDK_BDEV_IO_STATUS_NOMEM : SPDK_BDEV_IO_STATUS_FAILED);
    }
}

static int
raidx_start(struct raid_bdev *raid_bdev)
{
    struct raidx_info *rxinfo;
    struct raid_base_rpc_info *base_info;
    uint64_t min_blockcnt = kMaxBlockcnt;
    int ret;

    if (!raidx_validate_geometry(raid_bdev->num_base_rpcs, raid_bdev->num_parities)) {
        SPDK_ERRLOG("Invalid raidx geometry: total=%u parity=%u\n",
                    raid_bdev->num_base_rpcs, raid_bdev->num_parities);
        return -EINVAL;
    }

    rxinfo = (struct raidx_info *)calloc(1, sizeof(*rxinfo));
    if (rxinfo == NULL) {
        return -ENOMEM;
    }

    ret = raidx_ec_init(&rxinfo->ec, raid_bdev->num_data_chunks, raid_bdev->num_parities);
    if (ret != 0) {
        free(rxinfo);
        return ret;
    }

    RAID_FOR_EACH_BASE_RPC(raid_bdev, base_info) {
        min_blockcnt = spdk_min(min_blockcnt, base_info->blockcnt);
    }

    rxinfo->total_stripes = min_blockcnt / raid_bdev->strip_size;
    rxinfo->stripe_blocks = raid_bdev->strip_size * raid_bdev->num_data_chunks;

    raid_bdev->bdev.blockcnt = rxinfo->stripe_blocks * rxinfo->total_stripes;
    raid_bdev->bdev.optimal_io_boundary = rxinfo->stripe_blocks;
    raid_bdev->bdev.split_on_optimal_io_boundary = true;
    raid_bdev->module_private = rxinfo;
    return 0;
}

static void
raidx_stop(struct raid_bdev *raid_bdev)
{
    struct raidx_info *rxinfo = static_cast<struct raidx_info *>(raid_bdev->module_private);

    if (rxinfo == NULL) {
        return;
    }

    raidx_ec_fini(&rxinfo->ec);
    free(rxinfo);
    raid_bdev->module_private = NULL;
}

static int
raidx_io_channel_resource_init(struct raid_bdev *raid_bdev, void *resource)
{
    struct raidx_io_channel *rxch = static_cast<struct raidx_io_channel *>(resource);

    TAILQ_INIT(&rxch->retry_queue);
    return 0;
}

static void
raidx_io_channel_resource_deinit(void *resource)
{
    struct raidx_io_channel *rxch = static_cast<struct raidx_io_channel *>(resource);

    assert(TAILQ_EMPTY(&rxch->retry_queue));
}

static struct raid_bdev_module g_raidx_module = {
        .level = RAIDX,
        .base_rpcs_min = 2,
        .base_rpcs_max_degraded = kRaidxMaxParityRows,
        .io_channel_resource_size = sizeof(struct raidx_io_channel),
        .start = raidx_start,
        .stop = raidx_stop,
        .submit_rw_request = raidx_submit_rw_request,
        .io_channel_resource_init = raidx_io_channel_resource_init,
        .io_channel_resource_deinit = raidx_io_channel_resource_deinit,
};
RAID_MODULE_REGISTER(&g_raidx_module)

SPDK_LOG_REGISTER_COMPONENT(bdev_raidx)

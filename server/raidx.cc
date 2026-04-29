#include "../shared/common.h"

static unsigned char g_raidx_gf_tbl[256][32];

static char *g_bdev_name = (char*) "Nvme0n1";
static int g_process_id = -1;
static char *g_addr_file = (char*) "";
static uint32_t g_strip_size = 0;
static std::vector<std::string> ip_addrs;
static int8_t g_numOfServers = -1;
static uint8_t g_num_qp = 1;
static struct bdev_context_t *g_bdev_context;

static struct rte_hash *g_req_hash = NULL;
static struct rte_hash *g_rc_hash = NULL;

struct raidx_rc_hash_key {
    uint64_t offset;
    uint8_t data_index;
    uint8_t reserved[7];
};

static struct raidx_rc_hash_key
raidx_make_rc_hash_key(uint64_t offset, uint8_t data_index)
{
    struct raidx_rc_hash_key key = {};

    key.offset = offset;
    key.data_index = data_index;
    return key;
}

static int
raidx_rc_hash_lookup(uint64_t offset, uint8_t data_index, void **data)
{
    struct raidx_rc_hash_key key = raidx_make_rc_hash_key(offset, data_index);

    return rte_hash_lookup_data(g_rc_hash, &key, data);
}

static int
raidx_rc_hash_add(uint64_t offset, uint8_t data_index, void *data)
{
    struct raidx_rc_hash_key key = raidx_make_rc_hash_key(offset, data_index);

    return rte_hash_add_key_data(g_rc_hash, &key, data);
}

static int
raidx_rc_hash_del(uint64_t offset, uint8_t data_index)
{
    struct raidx_rc_hash_key key = raidx_make_rc_hash_key(offset, data_index);

    return rte_hash_del_key(g_rc_hash, &key);
}

static struct ibv_cq* g_cq; // global CQ
static struct rdma_qp_server *g_host_qp_list; // global host QP list
static struct rdma_qp_server *g_server_qp_list; // global server QP list

static struct spdk_poller *g_poller;

void rdma_send_resp(struct send_wr_wrapper_server *send_wrapper, uint64_t req_id);
void rdma_recv(struct recv_wr_wrapper_server *recv_wrapper);

void show_buffer(unsigned char *buf, long buffer_size){
    char tmp[555];
    long i, j;
    unsigned int b;
    for(i=0;i<buffer_size;i+=128){
        b=0;
        for(j=i;j<i+128 && j<buffer_size;++j){
            b+=sprintf(tmp+b, "%u ", buf[j]);
        }
        SPDK_NOTICELOG("%s\n", tmp);
    }
}

static void
raid5_xor_buf(void *to, void *from, size_t size)
{
    int ret;
    void *vects[3] = { from, to, to };

    ret = xor_gen(3, size, vects);
    if (ret) {
        SPDK_ERRLOG("xor_gen failed\n");
    }
}

static uint8_t
raidx_target_pos(enum target tgt)
{
    switch (tgt) {
        case PEER1:
            return 0;
        case PEER2:
            return 1;
        case PEER3:
            return 2;
        case PEER4:
            return 3;
        default:
            SPDK_ERRLOG("Illegal target enum: %d\n", tgt);
            assert(false);
    }
}

static char *
raidx_contribution_buffer(struct bdev_context_t *bdev_context, uint8_t target_pos)
{
    switch (target_pos) {
        case 0:
            return bdev_context->buff1;
        case 1:
            return bdev_context->buff3;
        case 2:
            return bdev_context->buff4;
        case 3:
            return bdev_context->buff5;
        default:
            SPDK_ERRLOG("Illegal target position: %u\n", target_pos);
            assert(false);
    }
}

static void
raidx_apply_recon_coeff(struct bdev_context_t *bdev_context, const struct cs_message_t *cs_msg)
{
    uint8_t coeff = cs_msg->recon_coeff;
    uint64_t offset = (cs_msg->fwd_offset % g_strip_size) << bdev_context->blk_size_shift;
    uint64_t length = cs_msg->fwd_length << bdev_context->blk_size_shift;

    if (length == 0 || coeff == 1) {
        return;
    }

    if (coeff == 0) {
        std::memset(bdev_context->buff1 + offset, 0, length);
        return;
    }

    gf_vect_mul(length, g_raidx_gf_tbl[coeff],
                bdev_context->buff1 + offset, bdev_context->buff2 + offset);
    std::memcpy(bdev_context->buff1 + offset, bdev_context->buff2 + offset, length);
}

static void
raidx_try_complete_partial_write(struct recv_wr_wrapper_server *recv_wrapper)
{
    struct bdev_context_t *bdev_context = recv_wrapper->bdev_context;
    struct rdma_qp_server *rqp;
    struct send_wr_wrapper_server *send_wrapper;

    if (!bdev_context->host_resp_sent &&
        bdev_context->io_done &&
        bdev_context->pending_send_completions == 0) {
        rqp = recv_wrapper->rqp;
        send_wrapper = TAILQ_FIRST(&rqp->free_send);
        if (spdk_unlikely(send_wrapper == NULL)) {
            SPDK_ERRLOG("run out of send buffers\n");
            assert(false);
        }
        TAILQ_REMOVE(&rqp->free_send, send_wrapper, link);
        send_wrapper->ctx = NULL;
        send_wrapper->await_callback = false;
        rdma_send_resp(send_wrapper, recv_wrapper->cs_msg->req_id);
        bdev_context->host_resp_sent = true;
    }

    if (bdev_context->host_resp_sent && bdev_context->pending_peer_callbacks == 0) {
        bdev_context->state = RELEASE;
        rdma_recv(recv_wrapper);
    }
}

struct bdev_context_t*
allocate_bdev_context(void)
{
    struct bdev_context_t* tmp = (struct bdev_context_t*) calloc(1, sizeof(struct bdev_context_t));
    tmp->bdev = g_bdev_context->bdev;
    tmp->bdev_thread = g_bdev_context->bdev_thread;
    tmp->bdev_desc = g_bdev_context->bdev_desc;
    tmp->bdev_io_channel = g_bdev_context->bdev_io_channel;
    tmp->bdev_name = g_bdev_context->bdev_name;
    tmp->blk_size = g_bdev_context->blk_size;
    tmp->blk_size_shift = g_bdev_context->blk_size_shift;
    tmp->buf_align = g_bdev_context->buf_align;
    tmp->buff1 = (char*) spdk_dma_zmalloc(g_strip_size * g_bdev_context->blk_size, g_bdev_context->buf_align, NULL);
    if (!tmp->buff1) {
        free(tmp);
        return NULL;
    }
    tmp->buff2 = (char*) spdk_dma_zmalloc(g_strip_size * g_bdev_context->blk_size, g_bdev_context->buf_align, NULL);
    if (!tmp->buff2) {
        spdk_dma_free(tmp->buff1);
        free(tmp);
        return NULL;
    }
    tmp->buff3 = (char*) spdk_dma_zmalloc(g_strip_size * g_bdev_context->blk_size, g_bdev_context->buf_align, NULL);
    if (!tmp->buff3) {
        spdk_dma_free(tmp->buff1);
        spdk_dma_free(tmp->buff2);
        free(tmp);
        return NULL;
    }
    tmp->buff4 = (char*) spdk_dma_zmalloc(g_strip_size * g_bdev_context->blk_size, g_bdev_context->buf_align, NULL);
    if (!tmp->buff4) {
        spdk_dma_free(tmp->buff1);
        spdk_dma_free(tmp->buff2);
        spdk_dma_free(tmp->buff3);
        free(tmp);
        return NULL;
    }
    tmp->buff5 = (char*) spdk_dma_zmalloc(g_strip_size * g_bdev_context->blk_size, g_bdev_context->buf_align, NULL);
    if (!tmp->buff5) {
        spdk_dma_free(tmp->buff1);
        spdk_dma_free(tmp->buff2);
        spdk_dma_free(tmp->buff3);
        spdk_dma_free(tmp->buff4);
        free(tmp);
        return NULL;
    }
    tmp->buff = tmp->buff1;
    return tmp;
}

void
free_bdev_context(struct bdev_context_t* ptr)
{
    if (!ptr) {
        return;
    }
    spdk_dma_free(ptr->buff1);
    spdk_dma_free(ptr->buff2);
    spdk_dma_free(ptr->buff3);
    spdk_dma_free(ptr->buff4);
    spdk_dma_free(ptr->buff5);
    free(ptr);
}

/*
 * This function is called to parse the parameters that are specific to this application
 */
int
bdev_parse_arg(int ch, char *arg)
{
    switch (ch) {
        case 'b':
            g_bdev_name = arg;
            break;
        case 'P':
            g_process_id = spdk_strtol(arg, 10) - 1;
            break;
        case 'a':
            g_addr_file = arg;
            break;
        case 'S':
            g_strip_size = (uint32_t) spdk_strtol(arg, 10);
            break;
        case 'N':
            g_numOfServers = (int8_t) spdk_strtol(arg, 10);
            break;
        case 'E':
            g_num_qp = (uint8_t) spdk_strtol(arg, 10);
            break;
    }
    return 0;
}

/*
 * Usage function for printing parameters that are specific to this application
 */
void
bdev_usage(void)
{
    printf(" -b <bdev>                 name of the bdev to use\n");
}

void
bdev_event_cb(enum spdk_bdev_event_type type, struct spdk_bdev *bdev,
              void *event_ctx)
{
    SPDK_NOTICELOG("Unsupported bdev event: type %d\n", type);
}

bool
need_read(uint8_t type)
{
    return type == RECON_RT;
}

void
run_state_machine(struct recv_wr_wrapper_server *recv_wrapper, enum action act);

void
io_complete(struct spdk_bdev_io *bdev_io, bool success, void *cb_arg)
{
    struct recv_wr_wrapper_server *recv_wrapper = (struct recv_wr_wrapper_server *) cb_arg;
    struct bdev_context_t *bdev_context = recv_wrapper->bdev_context;
    bdev_context->success = success;
    spdk_bdev_free_io(bdev_io);
    if (!success) {
        SPDK_ERRLOG("bdev io read error\n");
        spdk_put_io_channel(bdev_context->bdev_io_channel);
        spdk_bdev_close(bdev_context->bdev_desc);
        spdk_app_stop(-1);
    }
    run_state_machine(recv_wrapper, IO_COMPLETE);
}

void
bdev_read(void *arg)
{
    struct recv_wr_wrapper_server *recv_wrapper = (struct recv_wr_wrapper_server *) arg;
    struct bdev_context_t *bdev_context = recv_wrapper->bdev_context;
    int rc = 0;

    char *buff = bdev_context->buff + ((bdev_context->offset % g_strip_size) << bdev_context->blk_size_shift);

    rc = spdk_bdev_read_blocks(bdev_context->bdev_desc, bdev_context->bdev_io_channel,
                               buff, bdev_context->offset, bdev_context->size, io_complete, recv_wrapper);

    if (rc == -ENOMEM) {
        /* In case we cannot perform I/O now, queue I/O */
        bdev_context->bdev_io_wait.bdev = bdev_context->bdev;
        bdev_context->bdev_io_wait.cb_fn = bdev_read;
        bdev_context->bdev_io_wait.cb_arg = recv_wrapper;
        spdk_bdev_queue_io_wait(bdev_context->bdev, bdev_context->bdev_io_channel,
                                &bdev_context->bdev_io_wait);
    } else if (rc) {
        SPDK_ERRLOG("%s error while reading from bdev: %d\n", spdk_strerror(-rc), rc);
        spdk_put_io_channel(bdev_context->bdev_io_channel);
        spdk_bdev_close(bdev_context->bdev_desc);
        spdk_app_stop(-1);
    }
}

void
bdev_write(void *arg)
{
    struct recv_wr_wrapper_server *recv_wrapper = (struct recv_wr_wrapper_server *) arg;
    struct bdev_context_t *bdev_context = recv_wrapper->bdev_context;
    int rc = 0;

    char *buff = bdev_context->buff + ((bdev_context->offset % g_strip_size) << bdev_context->blk_size_shift);

    rc = spdk_bdev_write_blocks(bdev_context->bdev_desc, bdev_context->bdev_io_channel,
                                buff, bdev_context->offset, bdev_context->size, io_complete, recv_wrapper);

    if (rc == -ENOMEM) {
        /* In case we cannot perform I/O now, queue I/O */
        bdev_context->bdev_io_wait.bdev = bdev_context->bdev;
        bdev_context->bdev_io_wait.cb_fn = bdev_write;
        bdev_context->bdev_io_wait.cb_arg = recv_wrapper;
        spdk_bdev_queue_io_wait(bdev_context->bdev, bdev_context->bdev_io_channel,
                                &bdev_context->bdev_io_wait);
    } else if (rc) {
        SPDK_ERRLOG("%s error while writing to bdev: %d\n", spdk_strerror(-rc), rc);
        spdk_put_io_channel(bdev_context->bdev_io_channel);
        spdk_bdev_close(bdev_context->bdev_desc);
        spdk_app_stop(-1);
    }
}

void
rdma_write(struct send_wr_wrapper_server *send_wrapper, uint8_t wr_cnt)
{
    struct rdma_qp_server *rqp = send_wrapper->rqp;
    struct ibv_send_wr *send_wrs = send_wrapper->send_wr_write;
    struct ibv_send_wr *send_wr;
    for (uint8_t i = 0; i < wr_cnt; i++) {
        send_wr = &send_wrs[i];
        send_wr->next = NULL;
        if (rqp->send_wrs.first == NULL) {
            rqp->send_wrs.first = send_wr;
            rqp->send_wrs.last = send_wr;
        } else {
            rqp->send_wrs.last->next = send_wr;
            rqp->send_wrs.last = send_wr;
        }
        rqp->send_wrs.size++;
    }
}

void
rdma_read(struct recv_wr_wrapper_server *recv_wrapper, uint8_t wr_cnt)
{
    struct rdma_qp_server *rqp = recv_wrapper->rqp;
    struct ibv_send_wr *send_wrs = recv_wrapper->send_wr_read;
    struct ibv_send_wr *send_wr;
    for (uint8_t i = 0; i < wr_cnt; i++) {
        send_wr = &send_wrs[i];
        send_wr->next = NULL;
        if (rqp->send_wrs.first == NULL) {
            rqp->send_wrs.first = send_wr;
            rqp->send_wrs.last = send_wr;
        } else {
            rqp->send_wrs.last->next = send_wr;
            rqp->send_wrs.last = send_wr;
        }
        rqp->send_wrs.size++;
    }
}

void
rdma_send_msg(struct send_wr_wrapper_server *send_wrapper, struct recv_wr_wrapper_server *recv_wrapper, enum target tgt)
{
    struct rdma_qp_server *rqp = send_wrapper->rqp;
    struct cs_message_t *cs_msg = send_wrapper->cs_msg;
    struct cs_message_t *recv_cs_msg = recv_wrapper->cs_msg;
    struct bdev_context_t *bdev_context = recv_wrapper->bdev_context;
    struct ibv_send_wr *send_wr = &send_wrapper->send_wr;
    struct ibv_sge *sge = send_wr->sg_list;
    struct ibv_mr *mr;
    uint64_t size;
    uint8_t target_pos = raidx_target_pos(tgt);
    uint8_t coeff;
    char *contribution;
    uint64_t contribution_offset;

    switch (recv_cs_msg->type) {
        case FWD_RW_DIFF:
            assert(target_pos < recv_cs_msg->target_num);
            coeff = recv_cs_msg->coeff[target_pos];
            contribution = raidx_contribution_buffer(bdev_context, target_pos);
            contribution_offset = (recv_cs_msg->fwd_offset % g_strip_size) << bdev_context->blk_size_shift;

            if (coeff != 1) {
                gf_vect_mul(recv_cs_msg->fwd_length << bdev_context->blk_size_shift,
                            g_raidx_gf_tbl[coeff],
                            bdev_context->buff1 + contribution_offset,
                            contribution + contribution_offset);
            }

            size = sizeof(cs_message_t) - sizeof(struct sg_entry) * 31;
            cs_msg_init(cs_msg);
            cs_msg->type = SD_DF;
            cs_msg->length = recv_cs_msg->fwd_length;
            cs_msg->offset = recv_cs_msg->fwd_offset;
            cs_msg->data_index = recv_cs_msg->data_index;
            cs_msg->recon_coeff = 1;
            cs_msg->last_index = g_process_id;
            cs_msg->num_sge[0] = 1;
            cs_msg->num_sge[1] = 0;
            cs_msg->parity_num = 1;
            cs_msg->coeff_num = 1;
            cs_msg->parity_row[0] = recv_cs_msg->parity_row[target_pos];
            cs_msg->coeff[0] = coeff;
            cs_msg->sgl[0].addr = (uint64_t)((coeff == 1 ? bdev_context->buff1 : contribution) + contribution_offset);
            cs_msg->sgl[0].len = (uint32_t) cs_msg->length << bdev_context->blk_size_shift;
            mr = (struct ibv_mr *) spdk_mem_map_translate(rqp->mem_map, cs_msg->sgl[0].addr, NULL);
            cs_msg->sgl[0].rkey = mr->rkey;
            cs_msg->req_id = (uint64_t) send_wrapper;
            sge->length = size;
            send_wr->imm_data = kReqTypePeer;
            send_wr->send_flags = IBV_SEND_SIGNALED;
            break;
        case RECON_NRT:
        case RECON_RT:
        case RECON_RW_DIFF:
            contribution_offset = (recv_cs_msg->fwd_offset % g_strip_size) << bdev_context->blk_size_shift;
            size = sizeof(cs_message_t) - sizeof(struct sg_entry) * 31;
            cs_msg_init(cs_msg);
            cs_msg->type = SD_RC;
            cs_msg->length = recv_cs_msg->fwd_length;
            cs_msg->offset = recv_cs_msg->fwd_offset;
            cs_msg->data_index = recv_cs_msg->data_index;
            cs_msg->recon_coeff = 1;
            cs_msg->last_index = g_process_id;
            cs_msg->num_sge[0] = 1;
            cs_msg->num_sge[1] = 0;
            cs_msg->sgl[0].addr = (uint64_t)bdev_context->buff1 + contribution_offset;
            cs_msg->sgl[0].len = (uint32_t)cs_msg->length << bdev_context->blk_size_shift;
            mr = (struct ibv_mr *)spdk_mem_map_translate(rqp->mem_map, cs_msg->sgl[0].addr, NULL);
            cs_msg->sgl[0].rkey = mr->rkey;
            cs_msg->req_id = (uint64_t)send_wrapper;
            sge->length = size;
            send_wr->imm_data = kReqTypePeer;
            send_wr->send_flags = recv_cs_msg->type == RECON_RW_DIFF ? IBV_SEND_SIGNALED : 0;
            break;
        default:
            SPDK_ERRLOG("Illegal message type: %d\n", recv_cs_msg->type);
            assert(false);
    }

    send_wrapper->await_callback = true;
    send_wr->next = NULL;
    if (rqp->send_wrs.first == NULL) {
        rqp->send_wrs.first = send_wr;
        rqp->send_wrs.last = send_wr;
    } else {
        rqp->send_wrs.last->next = send_wr;
        rqp->send_wrs.last = send_wr;
    }
    rqp->send_wrs.size++;
}

void
rdma_send_resp(struct send_wr_wrapper_server *send_wrapper, uint64_t req_id)
{
    struct rdma_qp_server *rqp = send_wrapper->rqp;
    send_wrapper->await_callback = false;
    send_wrapper->cs_resp->req_id = req_id;
    struct ibv_send_wr *send_wr = &send_wrapper->send_wr;
    struct ibv_sge *sge = send_wr->sg_list;
    sge->length = sizeof(struct cs_resp_t);
    send_wr->imm_data = kReqTypeCallback;
    send_wr->send_flags = IBV_SEND_SIGNALED;

    send_wr->next = NULL;
    if (rqp->send_wrs.first == NULL) {
        rqp->send_wrs.first = send_wr;
        rqp->send_wrs.last = send_wr;
    } else {
        rqp->send_wrs.last->next = send_wr;
        rqp->send_wrs.last = send_wr;
    }
    rqp->send_wrs.size++;
}

void
rdma_recv(struct recv_wr_wrapper_server *recv_wrapper)
{
    struct rdma_qp_server *rqp = recv_wrapper->rqp;
    struct ibv_recv_wr *recv_wr = &recv_wrapper->recv_wr;

    recv_wr->next = NULL;
    if (rqp->recv_wrs.first == NULL) {
        rqp->recv_wrs.first = recv_wr;
        rqp->recv_wrs.last = recv_wr;
    } else {
        rqp->recv_wrs.last->next = recv_wr;
        rqp->recv_wrs.last = recv_wr;
    }
    rqp->recv_wrs.size++;
}

void
map_ibv_send_wr(struct ibv_send_wr *send_wrs, struct sg_entry *sges, uint8_t sge_cnt,
                uint64_t addr, uint64_t len, struct rdma_qp_server *rqp, bool signal)
{
    struct ibv_mr *mr = (struct ibv_mr *) spdk_mem_map_translate(rqp->mem_map, addr, NULL);
    struct ibv_send_wr *send_wr;
    struct ibv_sge *sge;
    uint64_t laddr_iter = addr;
    uint64_t llen = len;
    uint64_t send_len;
    for (uint8_t i = 0; i < sge_cnt; i++) {
        send_wr = &send_wrs[i];
        sge = send_wr->sg_list;

        send_len = spdk_min(sges[i].len, llen);
        sge->addr = laddr_iter;
        sge->length = send_len;
        sge->lkey = mr->lkey;
        send_wr->wr.rdma.remote_addr = sges[i].addr;
        send_wr->wr.rdma.rkey = sges[i].rkey;
        send_wr->send_flags = signal && i == sge_cnt - 1 ? IBV_SEND_SIGNALED : 0;
        laddr_iter += send_len;
        llen -= send_len;
    }
}

static void
raidx_start_recon_delta_read(struct recv_wr_wrapper_server *recv_wrapper)
{
    struct bdev_context_t *bdev_context = recv_wrapper->bdev_context;
    struct rdma_qp_server *rqp = recv_wrapper->rqp;
    uint64_t offset = (recv_wrapper->cs_msg->fwd_offset % g_strip_size) << bdev_context->blk_size_shift;
    uint64_t length = recv_wrapper->cs_msg->fwd_length << bdev_context->blk_size_shift;

    assert(recv_wrapper->cs_msg->num_sge[0] > 0);
    bdev_context->state = RECON_DIFF_READ_DONE;
    bdev_context->buff = bdev_context->buff2;
    map_ibv_send_wr(recv_wrapper->send_wr_read, recv_wrapper->cs_msg->sgl,
                    recv_wrapper->cs_msg->num_sge[0],
                    (uint64_t)bdev_context->buff2 + offset, length, rqp, true);
    rdma_read(recv_wrapper, recv_wrapper->cs_msg->num_sge[0]);
}

static void
raidx_apply_local_parity_delta(struct recv_wr_wrapper_server *recv_wrapper, uint8_t target_pos)
{
    struct bdev_context_t *bdev_context = recv_wrapper->bdev_context;
    struct cs_message_t *cs_msg = recv_wrapper->cs_msg;
    struct recv_wr_wrapper_server *parity_wrapper = nullptr;
    struct bdev_context_t *parity_context;
    uint64_t parity_offset = (cs_msg->y_offset % g_strip_size) << bdev_context->blk_size_shift;
    uint64_t parity_length = cs_msg->y_length << bdev_context->blk_size_shift;
    uint8_t coeff = cs_msg->coeff[target_pos];
    char *contribution = bdev_context->buff1;
    int rc;

    rc = rte_hash_lookup_data(g_req_hash, &cs_msg->y_offset, (void **)&parity_wrapper);
    if (spdk_unlikely(rc == -ENOENT)) {
        SPDK_ERRLOG("local parity target is not ready\n");
        assert(false);
    } else if (spdk_unlikely(rc < 0)) {
        assert(false);
    }

    if (coeff != 1) {
        contribution = raidx_contribution_buffer(bdev_context, target_pos);
        gf_vect_mul(parity_length, g_raidx_gf_tbl[coeff],
                    bdev_context->buff1 + parity_offset,
                    contribution + parity_offset);
    }

    parity_context = parity_wrapper->bdev_context;
    parity_context->recv_cnt++;
    raid5_xor_buf(parity_context->buff1 + parity_offset,
                  contribution + parity_offset, parity_length);
    if (parity_context->recv_cnt == parity_wrapper->cs_msg->fwd_num) {
        rc = rte_hash_del_key(g_req_hash, &cs_msg->y_offset);
        if (spdk_unlikely(rc < 0)) {
            assert(false);
        }
        bdev_write((void *)parity_wrapper);
    }
}

static void
raidx_finish_recon_request(struct recv_wr_wrapper_server *recv_wrapper)
{
    struct bdev_context_t *bdev_context = recv_wrapper->bdev_context;
    struct rdma_qp_server *rqp;
    struct send_wr_wrapper_server *send_wrapper;
    int8_t next_index = recv_wrapper->cs_msg->type == RECON_RW_DIFF ?
                        recv_wrapper->cs_msg->recon_next_index :
                        recv_wrapper->cs_msg->next_index;

    if (next_index < 0) {
        if (recv_wrapper->cs_msg->type == RECON_RW_DIFF) {
            raidx_start_recon_delta_read(recv_wrapper);
            return;
        }

        rqp = recv_wrapper->rqp;
        send_wrapper = TAILQ_FIRST(&rqp->free_send);
        if (spdk_unlikely(send_wrapper == NULL)) {
            SPDK_ERRLOG("run out of send buffers\n");
            assert(false);
        }
        TAILQ_REMOVE(&rqp->free_send, send_wrapper, link);
        send_wrapper->ctx = recv_wrapper;
        map_ibv_send_wr(send_wrapper->send_wr_write,
                        &recv_wrapper->cs_msg->sgl[recv_wrapper->cs_msg->num_sge[0]],
                        recv_wrapper->cs_msg->num_sge[1],
                        (uint64_t)bdev_context->buff1 +
                        ((recv_wrapper->cs_msg->fwd_offset % g_strip_size) << bdev_context->blk_size_shift),
                        recv_wrapper->cs_msg->fwd_length << bdev_context->blk_size_shift,
                        rqp, false);
        rdma_write(send_wrapper, recv_wrapper->cs_msg->num_sge[1]);
        rdma_send_resp(send_wrapper, recv_wrapper->cs_msg->req_id);
        return;
    }

    rqp = &g_server_qp_list[next_index];
    send_wrapper = TAILQ_FIRST(&rqp->free_send);
    if (spdk_unlikely(send_wrapper == NULL)) {
        SPDK_ERRLOG("run out of send buffers\n");
        assert(false);
    }
    TAILQ_REMOVE(&rqp->free_send, send_wrapper, link);
    send_wrapper->ctx = recv_wrapper;
    rdma_send_msg(send_wrapper, recv_wrapper, PEER1);
}

void
run_state_machine(struct recv_wr_wrapper_server *recv_wrapper, enum action act)
{
    struct bdev_context_t *bdev_context = recv_wrapper->bdev_context;
    struct rdma_qp_server *rqp;
    struct send_wr_wrapper_server *send_wrapper;
    struct recv_wr_wrapper_server *recv_wrapper_in_map = nullptr;
    enum request_state req_state = bdev_context->state;
    int rc;
    uint64_t left_bound, right_bound;
    switch (req_state) {
        case READ_START:
            bdev_context->state = READ_IO_DONE;
            bdev_context->buff = bdev_context->buff1;
            bdev_read((void*) recv_wrapper);
            break;
        case READ_IO_DONE:
            bdev_context->state = RELEASE;
            rqp = recv_wrapper->rqp;
            send_wrapper = TAILQ_FIRST(&rqp->free_send);
            if (spdk_unlikely(send_wrapper == NULL)) {
                SPDK_ERRLOG("run out of send buffers\n");
                assert(false);
            }
            TAILQ_REMOVE(&rqp->free_send, send_wrapper, link);
            send_wrapper->ctx = recv_wrapper;
            map_ibv_send_wr(send_wrapper->send_wr_write, recv_wrapper->cs_msg->sgl, recv_wrapper->cs_msg->num_sge[0],
                            (uint64_t) bdev_context->buff1 + ((bdev_context->offset % g_strip_size) << bdev_context->blk_size_shift),
                            bdev_context->size << bdev_context->blk_size_shift, rqp, false);
            rdma_write(send_wrapper, recv_wrapper->cs_msg->num_sge[0]);
            rdma_send_resp(send_wrapper, recv_wrapper->cs_msg->req_id);
            break;
        case WRITE_START:
            bdev_context->state = WRITE_READ_DONE;
            rqp = recv_wrapper->rqp;
            map_ibv_send_wr(recv_wrapper->send_wr_read, recv_wrapper->cs_msg->sgl, recv_wrapper->cs_msg->num_sge[0],
                            (uint64_t) bdev_context->buff1 + ((bdev_context->offset % g_strip_size) << bdev_context->blk_size_shift),
                            bdev_context->size << bdev_context->blk_size_shift, rqp, true);
            rdma_read(recv_wrapper, recv_wrapper->cs_msg->num_sge[0]);
            break;
        case WRITE_READ_DONE:
            bdev_context->state = WRITE_IO_DONE;
            bdev_context->buff = bdev_context->buff1;
            bdev_write((void*) recv_wrapper);
            break;
        case WRITE_IO_DONE:
            bdev_context->state = RELEASE;
            rqp = recv_wrapper->rqp;
            send_wrapper = TAILQ_FIRST(&rqp->free_send);
            if (spdk_unlikely(send_wrapper == NULL)) {
                SPDK_ERRLOG("run out of send buffers\n");
                assert(false);
            }
            TAILQ_REMOVE(&rqp->free_send, send_wrapper, link);
            send_wrapper->ctx = recv_wrapper;
            rdma_send_resp(send_wrapper, recv_wrapper->cs_msg->req_id);
            break;
        case FW_RO_START:
            bdev_context->state = FW_RO_IO_DONE;
            bdev_context->buff = bdev_context->buff1;
            bdev_read((void *) recv_wrapper);
            break;
        case FW_RO_IO_DONE:
            bdev_context->state = RELEASE;
            rqp = &g_server_qp_list[recv_wrapper->cs_msg->next_index];
            send_wrapper = TAILQ_FIRST(&rqp->free_send);
            if (spdk_unlikely(send_wrapper == NULL)) {
                SPDK_ERRLOG("run out of send buffers\n");
                assert(false);
            }
            TAILQ_REMOVE(&rqp->free_send, send_wrapper, link);
            send_wrapper->ctx = recv_wrapper;
            rdma_send_msg(send_wrapper, recv_wrapper, PEER1);
            break;
        case FW_RW_START:
            bdev_context->state = FW_RW_PEND1;
            bdev_context->buff = bdev_context->buff1;
            if (bdev_context->size == 0) {
                run_state_machine(recv_wrapper, IO_COMPLETE);
            } else {
                bdev_read((void *) recv_wrapper);
            }
            rqp = recv_wrapper->rqp;
            map_ibv_send_wr(recv_wrapper->send_wr_read, recv_wrapper->cs_msg->sgl, recv_wrapper->cs_msg->num_sge[0],
                            (uint64_t) bdev_context->buff1 + ((recv_wrapper->cs_msg->offset % g_strip_size) << bdev_context->blk_size_shift),
                            recv_wrapper->cs_msg->length << bdev_context->blk_size_shift, rqp, true);
            rdma_read(recv_wrapper, recv_wrapper->cs_msg->num_sge[0]);
            break;
        case FW_RW_PEND1:
            bdev_context->state = FW_RW_READ_IO1_DONE;
            break;
        case FW_RW_READ_IO1_DONE:
            bdev_context->state = FW_RW_PEND2;
            rqp = &g_server_qp_list[recv_wrapper->cs_msg->next_index];
            send_wrapper = TAILQ_FIRST(&rqp->free_send);
            if (spdk_unlikely(send_wrapper == NULL)) {
                SPDK_ERRLOG("run out of send buffers\n");
                assert(false);
            }
            TAILQ_REMOVE(&rqp->free_send, send_wrapper, link);
            send_wrapper->ctx = recv_wrapper;
            rdma_send_msg(send_wrapper, recv_wrapper, PEER1);
            bdev_context->offset = recv_wrapper->cs_msg->offset;
            bdev_context->size = recv_wrapper->cs_msg->length;
            bdev_write((void*) recv_wrapper);
            break;
        case FW_RW_PEND2:
            bdev_context->state = FW_RW_PEND3;
            if (act == IO_COMPLETE) {
                rqp = recv_wrapper->rqp;
                send_wrapper = TAILQ_FIRST(&rqp->free_send);
                if (spdk_unlikely(send_wrapper == NULL)) {
                    SPDK_ERRLOG("run out of send buffers\n");
                    assert(false);
                }
                TAILQ_REMOVE(&rqp->free_send, send_wrapper, link);
                send_wrapper->ctx = recv_wrapper;
                rdma_send_resp(send_wrapper, recv_wrapper->cs_msg->req_id);
            }
            break;
        case FW_RW_PEND3:
            bdev_context->state = RELEASE;
            if (act == IO_COMPLETE) {
                rqp = recv_wrapper->rqp;
                send_wrapper = TAILQ_FIRST(&rqp->free_send);
                if (spdk_unlikely(send_wrapper == NULL)) {
                    SPDK_ERRLOG("run out of send buffers\n");
                    assert(false);
                }
                TAILQ_REMOVE(&rqp->free_send, send_wrapper, link);
                send_wrapper->ctx = recv_wrapper;
                rdma_send_resp(send_wrapper, recv_wrapper->cs_msg->req_id);
            }
            break;
        case FW_DF_START:
            bdev_context->state = FW_DF_PEND1;
            bdev_context->buff = bdev_context->buff1;
            bdev_read((void *) recv_wrapper);
            rqp = recv_wrapper->rqp;
            map_ibv_send_wr(recv_wrapper->send_wr_read, recv_wrapper->cs_msg->sgl, recv_wrapper->cs_msg->num_sge[0],
                            (uint64_t) bdev_context->buff2 + ((bdev_context->offset % g_strip_size) << bdev_context->blk_size_shift),
                            bdev_context->size << bdev_context->blk_size_shift, rqp, true);
            rdma_read(recv_wrapper, recv_wrapper->cs_msg->num_sge[0]);
            break;
        case FW_DF_PEND1:
            bdev_context->state = FW_DF_READ_IO1_DONE;
            break;
        case FW_DF_READ_IO1_DONE:
            bdev_context->state = FW_DF_PEND2;
            raid5_xor_buf(bdev_context->buff1 + ((bdev_context->offset % g_strip_size) << bdev_context->blk_size_shift),
                          bdev_context->buff2 + ((bdev_context->offset % g_strip_size) << bdev_context->blk_size_shift),
                          bdev_context->size << bdev_context->blk_size_shift);
            bdev_context->buff = bdev_context->buff2;
            for (uint8_t peer_idx = 0; peer_idx < recv_wrapper->cs_msg->target_num; ++peer_idx) {
                rqp = &g_server_qp_list[(uint8_t)recv_wrapper->cs_msg->target_index[peer_idx]];
                send_wrapper = TAILQ_FIRST(&rqp->free_send);
                if (spdk_unlikely(send_wrapper == NULL)) {
                    SPDK_ERRLOG("run out of send buffers\n");
                    assert(false);
                }
                TAILQ_REMOVE(&rqp->free_send, send_wrapper, link);
                send_wrapper->ctx = recv_wrapper;
                rdma_send_msg(send_wrapper, recv_wrapper, (enum target)(PEER1 + peer_idx));
            }
            bdev_write((void*) recv_wrapper);
            break;
        case FW_DF_PEND2:
            if (act == IO_COMPLETE) {
                bdev_context->io_done = true;
            } else if (act == RDMA_SEND_COMPLETE) {
                assert(bdev_context->pending_send_completions > 0);
                bdev_context->pending_send_completions--;
            } else if (act == CALLBACK) {
                assert(bdev_context->pending_peer_callbacks > 0);
                bdev_context->pending_peer_callbacks--;
            }
            raidx_try_complete_partial_write(recv_wrapper);
            break;
        case FW_MIX_START:
            bdev_context->state = FW_MIX_PEND1;
            bdev_context->buff = bdev_context->buff1;
            bdev_read((void *) recv_wrapper);
            rqp = recv_wrapper->rqp;
            map_ibv_send_wr(recv_wrapper->send_wr_read, recv_wrapper->cs_msg->sgl, recv_wrapper->cs_msg->num_sge[0],
                            (uint64_t) bdev_context->buff2 + ((recv_wrapper->cs_msg->offset % g_strip_size) << bdev_context->blk_size_shift),
                            recv_wrapper->cs_msg->length << bdev_context->blk_size_shift, rqp, true);
            rdma_read(recv_wrapper, recv_wrapper->cs_msg->num_sge[0]);
            break;
        case FW_MIX_PEND1:
            bdev_context->state = FW_MIX_READ_IO1_DONE;
            break;
        case FW_MIX_READ_IO1_DONE:
            bdev_context->state = FW_MIX_PEND2;

            bdev_context->buff = bdev_context->buff2;
            bdev_context->offset = recv_wrapper->cs_msg->offset;
            bdev_context->size = recv_wrapper->cs_msg->length;

            left_bound = spdk_max(recv_wrapper->cs_msg->fwd_offset, recv_wrapper->cs_msg->offset);
            right_bound = spdk_min(recv_wrapper->cs_msg->fwd_offset + recv_wrapper->cs_msg->fwd_length,
                                   recv_wrapper->cs_msg->offset + recv_wrapper->cs_msg->length);
            if (left_bound > right_bound) {
                std::swap(left_bound, right_bound);
            }
            if (left_bound < right_bound) {
                std::memset(bdev_context->buff1 + ((left_bound % g_strip_size) << bdev_context->blk_size_shift),
                            0, (right_bound - left_bound) << bdev_context->blk_size_shift);
            }
            raid5_xor_buf(bdev_context->buff1 + ((bdev_context->offset % g_strip_size) << bdev_context->blk_size_shift),
                          bdev_context->buff2 + ((bdev_context->offset % g_strip_size) << bdev_context->blk_size_shift),
                          bdev_context->size << bdev_context->blk_size_shift);

            recv_wrapper->cs_msg->fwd_offset = bdev_context->offset - (bdev_context->offset % g_strip_size);
            recv_wrapper->cs_msg->fwd_length = g_strip_size;

            rqp = &g_server_qp_list[recv_wrapper->cs_msg->next_index];
            send_wrapper = TAILQ_FIRST(&rqp->free_send);
            if (spdk_unlikely(send_wrapper == NULL)) {
                SPDK_ERRLOG("run out of send buffers\n");
                assert(false);
            }
            TAILQ_REMOVE(&rqp->free_send, send_wrapper, link);
            send_wrapper->ctx = recv_wrapper;
            rdma_send_msg(send_wrapper, recv_wrapper, PEER1);
            bdev_write((void*) recv_wrapper);
            break;
        case FW_MIX_PEND2:
            bdev_context->state = FW_MIX_PEND3;
            if (act == IO_COMPLETE) {
                rqp = recv_wrapper->rqp;
                send_wrapper = TAILQ_FIRST(&rqp->free_send);
                if (spdk_unlikely(send_wrapper == NULL)) {
                    SPDK_ERRLOG("run out of send buffers\n");
                    assert(false);
                }
                TAILQ_REMOVE(&rqp->free_send, send_wrapper, link);
                send_wrapper->ctx = recv_wrapper;
                rdma_send_resp(send_wrapper, recv_wrapper->cs_msg->req_id);
            }
            break;
        case FW_MIX_PEND3:
            bdev_context->state = RELEASE;
            if (act == IO_COMPLETE) {
                rqp = recv_wrapper->rqp;
                send_wrapper = TAILQ_FIRST(&rqp->free_send);
                if (spdk_unlikely(send_wrapper == NULL)) {
                    SPDK_ERRLOG("run out of send buffers\n");
                    assert(false);
                }
                TAILQ_REMOVE(&rqp->free_send, send_wrapper, link);
                send_wrapper->ctx = recv_wrapper;
                rdma_send_resp(send_wrapper, recv_wrapper->cs_msg->req_id);
            }
            break;
        case PR_DF_START:
            bdev_context->state = PR_IO1_DONE;
            bdev_read((void*) recv_wrapper);
            break;
        case PR_IO1_DONE:
            bdev_context->state = PR_IO2_DONE;
            rc = rte_hash_lookup_data(g_req_hash, &bdev_context->offset, (void **) &recv_wrapper_in_map);
            if (rc == -ENOENT) {
                rc = rte_hash_add_key_data(g_req_hash, &bdev_context->offset, recv_wrapper);
                if (spdk_unlikely(rc < 0)) {
                    assert(false);
                }
                bdev_context->recv_cnt = 0;
            } else {
                bdev_context->recv_cnt = recv_wrapper_in_map->bdev_context->recv_cnt;
                raid5_xor_buf(bdev_context->buff1 + ((bdev_context->offset % g_strip_size) << bdev_context->blk_size_shift),
                              recv_wrapper_in_map->bdev_context->buff1 + ((bdev_context->offset % g_strip_size) << bdev_context->blk_size_shift),
                              bdev_context->size << bdev_context->blk_size_shift);
                rdma_recv(recv_wrapper_in_map);
                if (bdev_context->recv_cnt == recv_wrapper->cs_msg->fwd_num) {
                    rc = rte_hash_del_key(g_req_hash, &bdev_context->offset);
                    if (spdk_unlikely(rc < 0)) {
                        assert(false);
                    }
                    bdev_write((void*) recv_wrapper);
                } else {
                    rc = rte_hash_add_key_data(g_req_hash, &bdev_context->offset, recv_wrapper);
                    if (spdk_unlikely(rc < 0)) {
                        assert(false);
                    }
                }
            }
            break;
        case PR_IO2_DONE:
            bdev_context->state = RELEASE;
            rqp = recv_wrapper->rqp;
            send_wrapper = TAILQ_FIRST(&rqp->free_send);
            if (spdk_unlikely(send_wrapper == NULL)) {
                SPDK_ERRLOG("run out of send buffers\n");
                assert(false);
            }
            TAILQ_REMOVE(&rqp->free_send, send_wrapper, link);
            send_wrapper->ctx = recv_wrapper;
            rdma_send_resp(send_wrapper, recv_wrapper->cs_msg->req_id);
            break;
        case PR_MIX_START:
            bdev_context->state = PR_MIX_PEND1;
            bdev_context->buff = bdev_context->buff1;
            if (bdev_context->size == 0) {
                bdev_context->state = PR_IO1_DONE;
            } else {
                bdev_read((void *) recv_wrapper);
            }
            rqp = recv_wrapper->rqp;
            map_ibv_send_wr(recv_wrapper->send_wr_read, recv_wrapper->cs_msg->sgl, recv_wrapper->cs_msg->num_sge[0],
                            (uint64_t) bdev_context->buff1 + ((recv_wrapper->cs_msg->fwd_offset % g_strip_size) << bdev_context->blk_size_shift),
                            recv_wrapper->cs_msg->fwd_length << bdev_context->blk_size_shift, rqp, true);
            rdma_read(recv_wrapper, recv_wrapper->cs_msg->num_sge[0]);
            bdev_context->offset = recv_wrapper->cs_msg->offset;
            bdev_context->size = recv_wrapper->cs_msg->length;
            break;
        case PR_MIX_PEND1:
            bdev_context->state = PR_IO1_DONE;
            break;
        case SEND_DF_START:
            bdev_context->state = SEND_DF_READ_DONE;
            rqp = recv_wrapper->rqp;
            map_ibv_send_wr(recv_wrapper->send_wr_read, recv_wrapper->cs_msg->sgl, recv_wrapper->cs_msg->num_sge[0],
                            (uint64_t) bdev_context->buff1 + ((bdev_context->offset % g_strip_size) << bdev_context->blk_size_shift),
                            bdev_context->size << bdev_context->blk_size_shift, rqp, true);
            rdma_read(recv_wrapper, recv_wrapper->cs_msg->num_sge[0]);
            break;
        case SEND_DF_READ_DONE:
            rc = rte_hash_lookup_data(g_req_hash, &bdev_context->offset, (void **) &recv_wrapper_in_map);
            if (rc == -ENOENT) {
                rc = rte_hash_add_key_data(g_req_hash, &bdev_context->offset, recv_wrapper);
                if (spdk_unlikely(rc < 0)) {
                    assert(false);
                }
                bdev_context->recv_cnt = 1;
            } else {
                recv_wrapper_in_map->bdev_context->recv_cnt++;
                raid5_xor_buf(recv_wrapper_in_map->bdev_context->buff1 + ((bdev_context->offset % g_strip_size) << bdev_context->blk_size_shift),
                              bdev_context->buff1 + ((bdev_context->offset % g_strip_size) << bdev_context->blk_size_shift),
                              bdev_context->size << bdev_context->blk_size_shift);
                rdma_recv(recv_wrapper);
                if (recv_wrapper_in_map->bdev_context->recv_cnt == recv_wrapper_in_map->cs_msg->fwd_num &&
                    recv_wrapper_in_map->bdev_context->state != SEND_DF_READ_DONE) {
                    bdev_write((void*) recv_wrapper_in_map);
                    rc = rte_hash_del_key(g_req_hash, &recv_wrapper_in_map->bdev_context->offset);
                    if (spdk_unlikely(rc < 0)) {
                        assert(false);
                    }
                }
            }
            rqp = recv_wrapper->rqp;
            send_wrapper = TAILQ_FIRST(&rqp->free_send);
            if (spdk_unlikely(send_wrapper == NULL)) {
                SPDK_ERRLOG("run out of send buffers\n");
                assert(false);
            }
            TAILQ_REMOVE(&rqp->free_send, send_wrapper, link);
            send_wrapper->ctx = NULL;
            rdma_send_resp(send_wrapper, recv_wrapper->cs_msg->req_id);
            break;
        case RECON_START:
            bdev_context->state = RECON_IO_DONE;
            bdev_context->buff = bdev_context->buff1;
            bdev_read((void*) recv_wrapper);
            break;
        case RECON_IO_DONE:
            bdev_context->state = RELEASE;
            raidx_apply_recon_coeff(bdev_context, recv_wrapper->cs_msg);
            if (recv_wrapper->cs_msg->fwd_num == 0) {
                raidx_finish_recon_request(recv_wrapper);
                break;
            }

            rc = raidx_rc_hash_lookup(recv_wrapper->cs_msg->fwd_offset, recv_wrapper->cs_msg->data_index,
                                      (void **)&recv_wrapper_in_map);
            if (rc == -ENOENT) {
                rc = raidx_rc_hash_add(recv_wrapper->cs_msg->fwd_offset, recv_wrapper->cs_msg->data_index,
                                       recv_wrapper);
                if (spdk_unlikely(rc < 0)) {
                    assert(false);
                }
                bdev_context->recv_cnt = 0;
            } else {
                bdev_context->recv_cnt = recv_wrapper_in_map->bdev_context->recv_cnt;
                raid5_xor_buf(bdev_context->buff1 +
                              ((recv_wrapper->cs_msg->fwd_offset % g_strip_size) << bdev_context->blk_size_shift),
                              recv_wrapper_in_map->bdev_context->buff1 +
                              ((recv_wrapper->cs_msg->fwd_offset % g_strip_size) << bdev_context->blk_size_shift),
                              recv_wrapper->cs_msg->fwd_length << bdev_context->blk_size_shift);
                rdma_recv(recv_wrapper_in_map);
                if (bdev_context->recv_cnt == recv_wrapper->cs_msg->fwd_num) {
                    rc = raidx_rc_hash_del(recv_wrapper->cs_msg->fwd_offset, recv_wrapper->cs_msg->data_index);
                    if (spdk_unlikely(rc < 0)) {
                        assert(false);
                    }
                    raidx_finish_recon_request(recv_wrapper);
                } else {
                    rc = raidx_rc_hash_add(recv_wrapper->cs_msg->fwd_offset, recv_wrapper->cs_msg->data_index,
                                           recv_wrapper);
                    if (spdk_unlikely(rc < 0)) {
                        assert(false);
                    }
                }
            }
            break;
        case RECON_READ_START:
            bdev_context->state = RECON_READ_IO_DONE;
            bdev_context->buff = bdev_context->buff1;
            bdev_read((void*) recv_wrapper);
            break;
        case RECON_READ_IO_DONE:
            bdev_context->state = RECON_READ_PEND;
            if (recv_wrapper->cs_msg->fwd_num > 0) {
                bdev_context->buff = bdev_context->buff2;
                std::memcpy(bdev_context->buff2 +
                            ((recv_wrapper->cs_msg->offset % g_strip_size) << bdev_context->blk_size_shift),
                            bdev_context->buff1 +
                            ((recv_wrapper->cs_msg->offset % g_strip_size) << bdev_context->blk_size_shift),
                            recv_wrapper->cs_msg->length << bdev_context->blk_size_shift);
            }
            rqp = recv_wrapper->rqp;
            send_wrapper = TAILQ_FIRST(&rqp->free_send);
            if (spdk_unlikely(send_wrapper == NULL)) {
                SPDK_ERRLOG("run out of send buffers\n");
                assert(false);
            }
            TAILQ_REMOVE(&rqp->free_send, send_wrapper, link);
            send_wrapper->ctx = recv_wrapper;
            map_ibv_send_wr(send_wrapper->send_wr_write, recv_wrapper->cs_msg->sgl,
                            recv_wrapper->cs_msg->num_sge[0],
                            (uint64_t)bdev_context->buff +
                            ((recv_wrapper->cs_msg->offset % g_strip_size) << bdev_context->blk_size_shift),
                            recv_wrapper->cs_msg->length << bdev_context->blk_size_shift, rqp, false);
            rdma_write(send_wrapper, recv_wrapper->cs_msg->num_sge[0]);
            rdma_send_resp(send_wrapper, recv_wrapper->cs_msg->req_id);

            raidx_apply_recon_coeff(bdev_context, recv_wrapper->cs_msg);
            if (recv_wrapper->cs_msg->fwd_num == 0) {
                raidx_finish_recon_request(recv_wrapper);
                break;
            }

            rc = raidx_rc_hash_lookup(recv_wrapper->cs_msg->fwd_offset, recv_wrapper->cs_msg->data_index,
                                      (void **)&recv_wrapper_in_map);
            if (rc == -ENOENT) {
                rc = raidx_rc_hash_add(recv_wrapper->cs_msg->fwd_offset, recv_wrapper->cs_msg->data_index,
                                       recv_wrapper);
                if (spdk_unlikely(rc < 0)) {
                    assert(false);
                }
                bdev_context->recv_cnt = 0;
            } else {
                bdev_context->recv_cnt = recv_wrapper_in_map->bdev_context->recv_cnt;
                raid5_xor_buf(bdev_context->buff1 +
                              ((recv_wrapper->cs_msg->fwd_offset % g_strip_size) << bdev_context->blk_size_shift),
                              recv_wrapper_in_map->bdev_context->buff1 +
                              ((recv_wrapper->cs_msg->fwd_offset % g_strip_size) << bdev_context->blk_size_shift),
                              recv_wrapper->cs_msg->fwd_length << bdev_context->blk_size_shift);
                rdma_recv(recv_wrapper_in_map);
                if (bdev_context->recv_cnt == recv_wrapper->cs_msg->fwd_num) {
                    rc = raidx_rc_hash_del(recv_wrapper->cs_msg->fwd_offset, recv_wrapper->cs_msg->data_index);
                    if (spdk_unlikely(rc < 0)) {
                        assert(false);
                    }
                    raidx_finish_recon_request(recv_wrapper);
                } else {
                    rc = raidx_rc_hash_add(recv_wrapper->cs_msg->fwd_offset, recv_wrapper->cs_msg->data_index,
                                           recv_wrapper);
                    if (spdk_unlikely(rc < 0)) {
                        assert(false);
                    }
                }
            }
            break;
        case RECON_READ_PEND:
            bdev_context->state = RELEASE;
            break;
        case RECON_DF_START:
            bdev_context->state = RECON_DF_READ_DONE;
            rqp = recv_wrapper->rqp;
            map_ibv_send_wr(recv_wrapper->send_wr_read, recv_wrapper->cs_msg->sgl, recv_wrapper->cs_msg->num_sge[0],
                            (uint64_t) bdev_context->buff1 + ((bdev_context->offset % g_strip_size) << bdev_context->blk_size_shift),
                            bdev_context->size << bdev_context->blk_size_shift, rqp, true);
            rdma_read(recv_wrapper, recv_wrapper->cs_msg->num_sge[0]);
            break;
        case RECON_DF_READ_DONE:
            rc = raidx_rc_hash_lookup(bdev_context->offset, recv_wrapper->cs_msg->data_index,
                                      (void **)&recv_wrapper_in_map);
            if (rc == -ENOENT) {
                rc = raidx_rc_hash_add(bdev_context->offset, recv_wrapper->cs_msg->data_index, recv_wrapper);
                if (spdk_unlikely(rc < 0)) {
                    assert(false);
                }
                bdev_context->recv_cnt = 1;
            } else {
                recv_wrapper_in_map->bdev_context->recv_cnt++;
                raid5_xor_buf(recv_wrapper_in_map->bdev_context->buff1 +
                              ((bdev_context->offset % g_strip_size) << bdev_context->blk_size_shift),
                              bdev_context->buff1 +
                              ((bdev_context->offset % g_strip_size) << bdev_context->blk_size_shift),
                              bdev_context->size << bdev_context->blk_size_shift);
                rdma_recv(recv_wrapper);
                if (recv_wrapper_in_map->bdev_context->recv_cnt == recv_wrapper_in_map->cs_msg->fwd_num &&
                    recv_wrapper_in_map->bdev_context->state != RECON_DF_READ_DONE) {
                    raidx_finish_recon_request(recv_wrapper_in_map);
                    rc = raidx_rc_hash_del(recv_wrapper_in_map->cs_msg->fwd_offset,
                                           recv_wrapper_in_map->cs_msg->data_index);
                    if (spdk_unlikely(rc < 0)) {
                        assert(false);
                    }
                }
            }
            rqp = recv_wrapper->rqp;
            send_wrapper = TAILQ_FIRST(&rqp->free_send);
            if (spdk_unlikely(send_wrapper == NULL)) {
                SPDK_ERRLOG("run out of send buffers\n");
                assert(false);
            }
            TAILQ_REMOVE(&rqp->free_send, send_wrapper, link);
            send_wrapper->ctx = NULL;
            rdma_send_resp(send_wrapper, recv_wrapper->cs_msg->req_id);
            break;
        case RECON_DIFF_READ_DONE: {
            uint64_t recon_offset = (recv_wrapper->cs_msg->fwd_offset % g_strip_size) << bdev_context->blk_size_shift;
            uint8_t saved_type = recv_wrapper->cs_msg->type;
            uint64_t saved_fwd_offset = recv_wrapper->cs_msg->fwd_offset;
            uint64_t saved_fwd_length = recv_wrapper->cs_msg->fwd_length;

            bdev_context->state = RECON_DIFF_PEND;
            bdev_context->pending_send_completions = 0;
            bdev_context->pending_peer_callbacks = 0;
            bdev_context->io_done = true;
            bdev_context->host_resp_sent = false;
            raid5_xor_buf(bdev_context->buff1 + recon_offset,
                          bdev_context->buff2 + recon_offset,
                          recv_wrapper->cs_msg->fwd_length << bdev_context->blk_size_shift);

            recv_wrapper->cs_msg->type = FWD_RW_DIFF;
            recv_wrapper->cs_msg->fwd_offset = recv_wrapper->cs_msg->y_offset;
            recv_wrapper->cs_msg->fwd_length = recv_wrapper->cs_msg->y_length;
            for (uint8_t peer_idx = 0; peer_idx < recv_wrapper->cs_msg->target_num; ++peer_idx) {
                if (recv_wrapper->cs_msg->target_index[peer_idx] == g_process_id) {
                    raidx_apply_local_parity_delta(recv_wrapper, peer_idx);
                    continue;
                }
                rqp = &g_server_qp_list[(uint8_t)recv_wrapper->cs_msg->target_index[peer_idx]];
                send_wrapper = TAILQ_FIRST(&rqp->free_send);
                if (spdk_unlikely(send_wrapper == NULL)) {
                    SPDK_ERRLOG("run out of send buffers\n");
                    assert(false);
                }
                TAILQ_REMOVE(&rqp->free_send, send_wrapper, link);
                send_wrapper->ctx = recv_wrapper;
                bdev_context->pending_send_completions++;
                bdev_context->pending_peer_callbacks++;
                rdma_send_msg(send_wrapper, recv_wrapper, (enum target)(PEER1 + peer_idx));
            }
            recv_wrapper->cs_msg->type = saved_type;
            recv_wrapper->cs_msg->fwd_offset = saved_fwd_offset;
            recv_wrapper->cs_msg->fwd_length = saved_fwd_length;
            raidx_try_complete_partial_write(recv_wrapper);
            break;
        }
        case RECON_DIFF_PEND:
            if (act == RDMA_SEND_COMPLETE) {
                assert(bdev_context->pending_send_completions > 0);
                bdev_context->pending_send_completions--;
            } else if (act == CALLBACK) {
                assert(bdev_context->pending_peer_callbacks > 0);
                bdev_context->pending_peer_callbacks--;
            }
            raidx_try_complete_partial_write(recv_wrapper);
            break;
        case RELEASE:
            rdma_recv(recv_wrapper);
            break;
        default:
            SPDK_ERRLOG("Illegal state: %d\n", req_state);
            assert(false);
    }
}

void
req_handler_rw(struct recv_wr_wrapper_server *recv_wrapper)
{
    struct bdev_context_t *bdev_context = recv_wrapper->bdev_context;
    struct cs_message_t *cs_msg = recv_wrapper->cs_msg;

    bdev_context->offset = cs_msg->offset;
    bdev_context->size = cs_msg->length;

    switch (cs_msg->type) {
        case CS_WT:
            bdev_context->state = WRITE_START;
            break;
        case CS_RD:
            bdev_context->state = READ_START;
            break;
        default:
            SPDK_ERRLOG("Illegal message type: %d\n", cs_msg->type);
            assert(false);
    }
    run_state_machine(recv_wrapper, INIT);
}

void
req_handler_partial_write(struct recv_wr_wrapper_server *recv_wrapper)
{
    struct bdev_context_t* bdev_context = recv_wrapper->bdev_context;
    struct cs_message_t *cs_msg = recv_wrapper->cs_msg;
    uint64_t fwd_offset_bytes;

    switch (cs_msg->type) {
        case FWD_RW_DIFF:
            bdev_context->state = FW_DF_START;
            bdev_context->offset = cs_msg->offset;
            bdev_context->size = cs_msg->length;
            bdev_context->pending_send_completions = cs_msg->target_num;
            bdev_context->pending_peer_callbacks = cs_msg->target_num;
            bdev_context->io_done = false;
            bdev_context->host_resp_sent = false;
            fwd_offset_bytes = (cs_msg->fwd_offset % g_strip_size) << bdev_context->blk_size_shift;
            std::memset(bdev_context->buff1 + fwd_offset_bytes, 0,
                        cs_msg->fwd_length << bdev_context->blk_size_shift);
            break;
        default:
            SPDK_ERRLOG("Illegal message type: %d\n", cs_msg->type);
            assert(false);
    }
    run_state_machine(recv_wrapper, INIT);
}

void
req_handler_parity(struct recv_wr_wrapper_server *recv_wrapper)
{
    struct bdev_context_t* bdev_context = recv_wrapper->bdev_context;
    struct cs_message_t *cs_msg = recv_wrapper->cs_msg;

    bdev_context->size = cs_msg->length;
    bdev_context->offset = cs_msg->offset;
    bdev_context->buff = bdev_context->buff1;
    switch(cs_msg->type) {
        case PR_DIFF:
            bdev_context->state = PR_DF_START;
            break;
        default:
            SPDK_ERRLOG("Illegal message type: %d\n", cs_msg->type);
            assert(false);
    }
    run_state_machine(recv_wrapper, INIT);
}

void
req_handler_rcread(struct recv_wr_wrapper_server *recv_wrapper)
{
    struct bdev_context_t* bdev_context = recv_wrapper->bdev_context;
    struct cs_message_t *cs_msg = recv_wrapper->cs_msg;

    if (need_read(cs_msg->type)) {
        bdev_context->offset = spdk_min(cs_msg->offset, cs_msg->fwd_offset);
        bdev_context->size = spdk_max(cs_msg->offset + cs_msg->length, cs_msg->fwd_offset + cs_msg->fwd_length)
                             - bdev_context->offset;
        bdev_context->state = RECON_READ_START;
    } else {
        bdev_context->offset = cs_msg->fwd_offset;
        bdev_context->size = cs_msg->fwd_length;
        bdev_context->state = RECON_START;
    }

    if (cs_msg->type == RECON_RW_DIFF && cs_msg->recon_next_index < 0) {
        uint64_t parity_offset = (cs_msg->y_offset % g_strip_size) << bdev_context->blk_size_shift;
        uint64_t parity_length = cs_msg->y_length << bdev_context->blk_size_shift;

        std::memset(bdev_context->buff1 + parity_offset, 0, parity_length);
        std::memset(bdev_context->buff2 + parity_offset, 0, parity_length);
    }

    run_state_machine(recv_wrapper, INIT);
}

void
req_handler_peer(struct recv_wr_wrapper_server *recv_wrapper)
{
    struct bdev_context_t* bdev_context = recv_wrapper->bdev_context;
    struct cs_message_t *cs_msg = recv_wrapper->cs_msg;

    bdev_context->size = cs_msg->length;
    bdev_context->offset = cs_msg->offset;
    switch (cs_msg->type) {
        case SD_DF:
            bdev_context->state = SEND_DF_START;
            break;
        case SD_RC:
            bdev_context->state = RECON_DF_START;
            break;
        default:
            SPDK_ERRLOG("Illegal message type: %d\n", cs_msg->type);
            assert(false);
    }
    run_state_machine(recv_wrapper, INIT);
}

void
req_handler_callback(struct recv_wr_wrapper_server *recv_wrapper)
{
    struct cs_resp_t *cs_resp = recv_wrapper->cs_resp;
    struct send_wr_wrapper_server *send_wrapper_release = (struct send_wr_wrapper_server *) cs_resp->req_id;
    struct recv_wr_wrapper_server *recv_wrapper_release = send_wrapper_release->ctx;
    send_wrapper_release->await_callback = false;
    if (recv_wrapper_release != NULL) {
        run_state_machine(recv_wrapper_release, CALLBACK);
    }
    TAILQ_INSERT_TAIL(&send_wrapper_release->rqp->free_send, send_wrapper_release, link);
    rdma_recv(recv_wrapper);
}

int
poll_cq(void *arg)
{
    struct ibv_wc wc[batch_size];
    uint8_t msg_type;
    struct send_wr_wrapper_server *send_wrapper;
    struct recv_wr_wrapper_server *recv_wrapper;
    struct ibv_recv_wr *recv_wr;
    struct ibv_recv_wr *bad_recv_wr;
    struct ibv_send_wr *bad_send_wr;
    int i, ret;
    uint64_t id;
    struct rdma_qp_server *rqp;

    int num_cqe = ibv_poll_cq(g_cq, batch_size, wc);

    for (i = 0; i < num_cqe; i++) {
        if (spdk_unlikely(wc[i].status != IBV_WC_SUCCESS)) {
            SPDK_ERRLOG("wc status error %u\n", wc[i].status);
            assert(false);
        }
        id = wc[i].wr_id;
        switch (wc[i].opcode) {
            case IBV_WC_SEND:
                send_wrapper = (struct send_wr_wrapper_server *) id;
                if (send_wrapper->ctx != NULL) {
                    if (!(send_wrapper->await_callback &&
                          send_wrapper->ctx->bdev_context->state == RELEASE)) {
                        run_state_machine(send_wrapper->ctx, RDMA_SEND_COMPLETE);
                    }
                }
                if (!send_wrapper->await_callback) {
                    TAILQ_INSERT_TAIL(&send_wrapper->rqp->free_send, send_wrapper, link);
                }
                break;
            case IBV_WC_RDMA_READ:
                recv_wrapper = (struct recv_wr_wrapper_server *) id;
                run_state_machine(recv_wrapper, RDMA_READ_COMPLETE);
                break;
            case IBV_WC_RECV:
                recv_wrapper = (struct recv_wr_wrapper_server *) id;
                msg_type = wc[i].imm_data;
                switch (msg_type) {
                    case kReqTypeRW:
                        req_handler_rw(recv_wrapper);
                        break;
                    case kReqTypePartialWrite:
                        req_handler_partial_write(recv_wrapper);
                        break;
                    case kReqTypeParity:
                        req_handler_parity(recv_wrapper);
                        break;
                    case kReqTypeReconRead:
                        req_handler_rcread(recv_wrapper);
                        break;
                    case kReqTypePeer:
                        req_handler_peer(recv_wrapper);
                        break;
                    case kReqTypeCallback:
                        req_handler_callback(recv_wrapper);
                        break;
                    default:
                        SPDK_ERRLOG("Illegal imm_data type: %d\n", msg_type);
                        assert(false);
                }
                break;
            default:
                SPDK_ERRLOG("Illegal RDMA opcode: %d\n", wc[i].opcode);
                assert(false);
        }
    }

    for (i = 0; i < g_num_qp; i++) {
        rqp = &g_host_qp_list[i];

        if (rqp->send_wrs.first != NULL) {
            if (spdk_unlikely(ibv_post_send(rqp->cmd_cm_id->qp, rqp->send_wrs.first, &bad_send_wr))) {
                SPDK_ERRLOG("post send failed\n");
                assert(false);
            }
            num_cqe += rqp->send_wrs.size;
            rqp->send_wrs.first = NULL;
            rqp->send_wrs.size = 0;
        }
        if (rqp->recv_wrs.first != NULL) {
            if (spdk_unlikely(ibv_post_recv(rqp->cmd_cm_id->qp, rqp->recv_wrs.first, &bad_recv_wr))) {
                SPDK_ERRLOG("post recv failed\n");
                assert(false);
            }
            num_cqe += rqp->recv_wrs.size;
            rqp->recv_wrs.first = NULL;
            rqp->recv_wrs.size = 0;
        }
    }
    for (i = 0; i < g_numOfServers; i++) {
        if (i == g_process_id) {
            continue;
        }
        rqp = &g_server_qp_list[i];
        if (rqp->send_wrs.first != NULL) {
            ret = ibv_post_send(rqp->cmd_cm_id->qp, rqp->send_wrs.first, &bad_send_wr);
            if (spdk_unlikely(ret)) {
                SPDK_ERRLOG("post send failed\n");
                if (ret == EINVAL || ret == -EINVAL) {
                    SPDK_ERRLOG("EINVAL\n");
                } else if (ret == ENOMEM || ret == -ENOMEM){
                    SPDK_ERRLOG("ENOMEM\n");
                } else if (ret == EFAULT || ret == -EFAULT) {
                    SPDK_ERRLOG("EFAULT\n");
                } else {
                    SPDK_ERRLOG("ret=%d, errno=%d\n", ret, errno);
                }
                assert(false);
            }
            num_cqe += rqp->send_wrs.size;
            rqp->send_wrs.first = NULL;
            rqp->send_wrs.size = 0;
        }
        if (rqp->recv_wrs.first != NULL) {
            if (spdk_unlikely(ibv_post_recv(rqp->cmd_cm_id->qp, rqp->recv_wrs.first, &bad_recv_wr))) {
                SPDK_ERRLOG("post recv failed\n");
                assert(false);
            }
            num_cqe += rqp->recv_wrs.size;
            rqp->recv_wrs.first = NULL;
            rqp->recv_wrs.size = 0;
        }
    }

    return num_cqe > 0 ? SPDK_POLLER_BUSY : SPDK_POLLER_IDLE;
}

void
connect_rdma_connections(void)
{
    struct rdma_event_channel **svr_cmd_cm_channel_list; // cmd channels for servers
    struct rdma_event_channel **host_cmd_cm_channel_list; // cmd channels for hosts
    struct rdma_cm_event* event;
    struct rdma_conn_param conn_param = {};
    conn_param.initiator_depth = 16;
    conn_param.responder_resources = 16;
    conn_param.retry_count = 4;
    conn_param.rnr_retry_count = 7;
    struct rdma_cm_id* svr_cmd_cm_id_list[g_numOfServers];
    struct ibv_pd* svr_cmd_pd_list[g_numOfServers];
    struct addrinfo* svr_cmd_dst[g_numOfServers];
    struct addrinfo* svr_cmd_src[g_numOfServers];
    struct rdma_cm_id* host_cmd_cm_id_list[g_num_qp];
    struct ibv_pd* host_cmd_pd_list[g_num_qp];
    struct addrinfo* host_cmd_dst[g_num_qp];
    struct addrinfo* host_cmd_src[g_num_qp];
    struct addrinfo	hints = {
            .ai_family    = AF_INET, // ipv4
            .ai_socktype  = SOCK_STREAM,
            .ai_protocol  = 0
    };
    struct ibv_qp_init_attr	qp_attr = {};
    int ret, i, j, k;

    struct ibv_recv_wr *bad_recv_wr;

    g_server_qp_list = (struct rdma_qp_server *) calloc(g_numOfServers, sizeof(struct rdma_qp_server));
    if (!g_server_qp_list) {
        SPDK_ERRLOG("failed to allocate g_server_qp_list\n");
        assert(false);
    }
    g_host_qp_list = (struct rdma_qp_server *) calloc(g_num_qp, sizeof(struct rdma_qp_server));
    if (!g_host_qp_list) {
        SPDK_ERRLOG("failed to allocate g_host_qp_list\n");
        assert(false);
    }

    // Allocate buffer
    // servers
    for (i = 0; i < g_numOfServers; i++) {
        if (i == g_process_id) {
            continue;
        }

        g_server_qp_list[i].cmd_sendbuf = (uint8_t *) calloc(paraSend * g_num_qp * 2, bufferSize);
        if (!g_server_qp_list[i].cmd_sendbuf) {
            SPDK_ERRLOG("allocate buffer failed\n");
            assert(false);
        }
        g_server_qp_list[i].cmd_recvbuf = g_server_qp_list[i].cmd_sendbuf + paraSend * g_num_qp * bufferSize;

        TAILQ_INIT(&g_server_qp_list[i].free_send);
        for (j = 0; j < paraSend * g_num_qp; j++) {
            struct ibv_sge *send_sge = (struct ibv_sge *) calloc(1, sizeof(struct ibv_sge));
            if (!send_sge) {
                SPDK_ERRLOG("failed to allocate send_sge\n");
                assert(false);
            }
            struct send_wr_wrapper_server *send_wrapper = (struct send_wr_wrapper_server *) calloc(1, sizeof(struct send_wr_wrapper_server));
            if (!send_wrapper) {
                SPDK_ERRLOG("failed to allocate send_wrapper\n");
                assert(false);
            }
            struct ibv_send_wr *send_wr = &send_wrapper->send_wr;
            struct ibv_send_wr *write_wr = send_wrapper->send_wr_write;
            send_wrapper->rqp = &g_server_qp_list[i];
            send_wr->opcode = IBV_WR_SEND_WITH_IMM;
            send_wr->sg_list = send_sge;
            send_wr->num_sge = 1;
            send_sge->addr = (uint64_t) g_server_qp_list[i].cmd_sendbuf + j * bufferSize;
            send_wrapper->cs_msg = (struct cs_message_t *) send_sge->addr;
            send_wrapper->cs_resp = (struct cs_resp_t *) send_sge->addr;
            send_sge->length = bufferSize;
            send_wr->send_flags = IBV_SEND_SIGNALED;
            send_wr->wr_id = (uint64_t) send_wrapper;
            struct cs_message_t *cs_msg = (struct cs_message_t *) send_sge->addr;
            cs_msg->req_id = (uint64_t) send_wrapper;
            TAILQ_INSERT_TAIL(&g_server_qp_list[i].free_send, send_wrapper, link);

            struct ibv_sge *recv_sge = (struct ibv_sge *) calloc(1, sizeof(struct ibv_sge));
            if (!recv_sge) {
                SPDK_ERRLOG("failed to allocate recv_sge\n");
                assert(false);
            }
            struct recv_wr_wrapper_server *recv_wrapper = (struct recv_wr_wrapper_server *) calloc(1, sizeof(struct recv_wr_wrapper_server));
            if (!recv_wrapper) {
                SPDK_ERRLOG("failed to allocate recv_wrapper\n");
                assert(false);
            }
            struct ibv_recv_wr *recv_wr = &recv_wrapper->recv_wr;
            struct ibv_send_wr *read_wr = recv_wrapper->send_wr_read;
            recv_wrapper->rqp = &g_server_qp_list[i];
            recv_sge->addr = (uint64_t) g_server_qp_list[i].cmd_recvbuf + j * bufferSize;
            recv_wrapper->cs_msg = (struct cs_message_t *) recv_sge->addr;
            recv_wrapper->cs_resp = (struct cs_resp_t *) recv_sge->addr;
            recv_sge->length = bufferSize;
            recv_wr->wr_id = (uint64_t) recv_wrapper;
            recv_wr->sg_list = recv_sge;
            recv_wr->num_sge = 1;
            struct ibv_sge *read_sge = (struct ibv_sge *) calloc(1, sizeof(struct ibv_sge));
            if (!read_sge) {
                SPDK_ERRLOG("failed to allocate read_sge\n");
                assert(false);
            }
            read_wr[0].sg_list = read_sge;
            read_wr[0].num_sge = 1;
            read_wr[0].wr_id = (uint64_t) recv_wrapper;
            read_wr[0].send_flags = 0;
            read_wr[0].opcode = IBV_WR_RDMA_READ;

            if (g_server_qp_list[i].recv_wrs.first == NULL) {
                g_server_qp_list[i].recv_wrs.first = recv_wr;
                g_server_qp_list[i].recv_wrs.last = recv_wr;
            } else {
                g_server_qp_list[i].recv_wrs.last->next = recv_wr;
                g_server_qp_list[i].recv_wrs.last = recv_wr;
            }
        }
    }
    // host
    for (i = 0; i < g_num_qp; i++) {
        g_host_qp_list[i].cmd_sendbuf = (uint8_t *) calloc(paraSend * 2, bufferSize);
        if (!g_host_qp_list[i].cmd_sendbuf) {
            SPDK_ERRLOG("allocate buffer failed\n");
            assert(false);
        }
        g_host_qp_list[i].cmd_recvbuf = g_host_qp_list[i].cmd_sendbuf + paraSend * bufferSize;

        TAILQ_INIT(&g_host_qp_list[i].free_send);
        for (j = 0; j < paraSend; j++) {
            struct ibv_sge *send_sge = (struct ibv_sge *) calloc(1, sizeof(struct ibv_sge));
            if (!send_sge) {
                SPDK_ERRLOG("failed to allocate send_sge\n");
                assert(false);
            }
            struct send_wr_wrapper_server *send_wrapper = (struct send_wr_wrapper_server *) calloc(1, sizeof(struct send_wr_wrapper_server));
            if (!send_wrapper) {
                SPDK_ERRLOG("failed to allocate send_wrapper\n");
                assert(false);
            }
            struct ibv_send_wr *send_wr = &send_wrapper->send_wr;
            struct ibv_send_wr *write_wr = send_wrapper->send_wr_write;
            send_wrapper->rqp = &g_host_qp_list[i];
            send_wr->opcode = IBV_WR_SEND_WITH_IMM;
            send_wr->sg_list = send_sge;
            send_wr->num_sge = 1;
            send_sge->addr = (uint64_t) g_host_qp_list[i].cmd_sendbuf + j * bufferSize;
            send_wrapper->cs_msg = (struct cs_message_t *) send_sge->addr;
            send_wrapper->cs_resp = (struct cs_resp_t *) send_sge->addr;
            send_sge->length = bufferSize;
            send_wr->send_flags = IBV_SEND_SIGNALED;
            send_wr->wr_id = (uint64_t) send_wrapper;
            struct cs_message_t *cs_msg = (struct cs_message_t *) send_sge->addr;
            cs_msg->req_id = (uint64_t) send_wrapper;
            for (k = 0; k < 16; k++) {
                struct ibv_sge *write_sge = (struct ibv_sge *) calloc(1, sizeof(struct ibv_sge));
                if (!write_sge) {
                    SPDK_ERRLOG("failed to allocate write_sge\n");
                    assert(false);
                }
                write_wr[k].sg_list = write_sge;
                write_wr[k].num_sge = 1;
                write_wr[k].wr_id = 0;
                write_wr[k].send_flags = 0;
                write_wr[k].opcode = IBV_WR_RDMA_WRITE;
            }
            TAILQ_INSERT_TAIL(&g_host_qp_list[i].free_send, send_wrapper, link);

            struct ibv_sge *recv_sge = (struct ibv_sge *) calloc(1, sizeof(struct ibv_sge));
            if (!recv_sge) {
                SPDK_ERRLOG("failed to allocate recv_sge\n");
                assert(false);
            }
            struct recv_wr_wrapper_server *recv_wrapper = (struct recv_wr_wrapper_server *) calloc(1, sizeof(struct recv_wr_wrapper_server));
            if (!recv_wrapper) {
                SPDK_ERRLOG("failed to allocate recv_wrapper\n");
                assert(false);
            }
            struct ibv_recv_wr *recv_wr = &recv_wrapper->recv_wr;
            struct ibv_send_wr *read_wr = recv_wrapper->send_wr_read;
            recv_wrapper->rqp = &g_host_qp_list[i];
            recv_sge->addr = (uint64_t) g_host_qp_list[i].cmd_recvbuf + j * bufferSize;
            recv_wrapper->cs_msg = (struct cs_message_t *) recv_sge->addr;
            recv_wrapper->cs_resp = (struct cs_resp_t *) recv_sge->addr;
            recv_sge->length = bufferSize;
            recv_wr->wr_id = (uint64_t) recv_wrapper;
            recv_wr->sg_list = recv_sge;
            recv_wr->num_sge = 1;
            for (k = 0; k < 16; k++) {
                struct ibv_sge *read_sge = (struct ibv_sge *) calloc(1, sizeof(struct ibv_sge));
                if (!read_sge) {
                    SPDK_ERRLOG("failed to allocate read_sge\n");
                    assert(false);
                }
                read_wr[k].sg_list = read_sge;
                read_wr[k].num_sge = 1;
                read_wr[k].wr_id = (uint64_t) recv_wrapper;
                read_wr[k].send_flags = 0;
                read_wr[k].opcode = IBV_WR_RDMA_READ;
            }

            if (g_host_qp_list[i].recv_wrs.first == NULL) {
                g_host_qp_list[i].recv_wrs.first = recv_wr;
                g_host_qp_list[i].recv_wrs.last = recv_wr;
            } else {
                g_host_qp_list[i].recv_wrs.last->next = recv_wr;
                g_host_qp_list[i].recv_wrs.last = recv_wr;
            }
        }
    }

    // g_cq 延迟到 rdma_bind_addr 之后用 cm_id->verbs 创建，避免 contexts[] 索引错误导致设备不匹配
    g_cq = NULL;

    /* Setup event channel */
    svr_cmd_cm_channel_list = (struct rdma_event_channel **) calloc(g_numOfServers, sizeof(struct rdma_event_channel *));
    if (!svr_cmd_cm_channel_list) {
        SPDK_ERRLOG("failed to allocate svr_cmd_cm_channel_list\n");
        assert(false);
    }
    host_cmd_cm_channel_list = (struct rdma_event_channel **) calloc(g_num_qp, sizeof(struct rdma_event_channel *));
    if (!host_cmd_cm_channel_list) {
        SPDK_ERRLOG("failed to allocate host_cmd_cm_channel_list\n");
        assert(false);
    }

    /* Create event channels */
    for (i = 0; i < g_numOfServers; i++) {
        if (i == g_process_id) {
            continue;
        }
        svr_cmd_cm_channel_list[i] = rdma_create_event_channel();
        if (!svr_cmd_cm_channel_list[i]) {
            SPDK_ERRLOG("Creating event channel failed, errno: %d \n", -errno);
            assert(false);
        }
    }
    for (i = 0; i < g_num_qp; i++) {
        host_cmd_cm_channel_list[i] = rdma_create_event_channel();
        if (!host_cmd_cm_channel_list[i]) {
            SPDK_ERRLOG("Creating event channel failed, errno: %d \n", -errno);
            assert(false);
        }
    }

    /* Listen */
    // servers 4620, 4621, ....
    for (i = 0; i < g_numOfServers; i++) {
        if (i == g_process_id) {
            continue;
        }
        ret = getaddrinfo(ip_addrs[g_process_id+1].c_str(), std::to_string(kStartPort+i).c_str(), &hints, &svr_cmd_src[i]);
        if (ret) {
            SPDK_ERRLOG("getaddrinfo for src failed\n");
            assert(false);
        }
        ret = rdma_create_id(svr_cmd_cm_channel_list[i], &svr_cmd_cm_id_list[i], NULL, RDMA_PS_TCP);
        if (ret) {
            SPDK_ERRLOG("Creating cm id failed with errno: %d \n", -errno);
            assert(false);
        }
        if (i > g_process_id) {
            ret = rdma_bind_addr(svr_cmd_cm_id_list[i], svr_cmd_src[i]->ai_addr);
            if (ret) {
                SPDK_ERRLOG("svr bind_addr failed: errno=%d (%s), addr=%s:%d\n",
                            errno, strerror(errno),
                            ip_addrs[g_process_id+1].c_str(), kStartPort+i);
                assert(false);
            }
            ret = rdma_listen(svr_cmd_cm_id_list[i], 10);
            if (ret) {
                SPDK_ERRLOG("listen failed\n");
                assert(false);
            }
        }
    }
    // host 4619, 4618, ....
    for (i = 0; i < g_num_qp; i++) {
        ret = getaddrinfo(ip_addrs[g_process_id+1].c_str(), std::to_string(kStartPort-1-i).c_str(), &hints, &host_cmd_src[i]);
        if (ret) {
            SPDK_ERRLOG("getaddrinfo for src failed\n");
            assert(false);
        }
        ret = rdma_create_id(host_cmd_cm_channel_list[i], &host_cmd_cm_id_list[i], NULL, RDMA_PS_TCP);
        if (ret) {
            SPDK_ERRLOG("Creating cm id failed with errno: %d \n", -errno);
            assert(false);
        }

        ret = rdma_bind_addr(host_cmd_cm_id_list[i], host_cmd_src[i]->ai_addr);
        if (ret) {
            SPDK_ERRLOG("bind_addr failed\n");
            assert(false);
        }
        // 首次 bind 后，从 cm_id->verbs 获取实际选中的 RDMA 设备，创建全局 CQ
        // 避免 contexts[] 硬编码索引导致设备错配（如 192.168.1.x 返回 contexts[1] 而非 contexts[0]）
        if (g_cq == NULL) {
            g_cq = ibv_create_cq(host_cmd_cm_id_list[i]->verbs, paraSend * g_num_qp * 2, NULL, NULL, 0);
            assert(g_cq != NULL);
        }
        ret = rdma_listen(host_cmd_cm_id_list[i], 10);
        if (ret) {
            SPDK_ERRLOG("listen failed\n");
            assert(false);
        }
    }

    /* connect to servers */
    for (i = 0; i < g_process_id; i++) {
        // i should be ready, resolve address
        ret = getaddrinfo(ip_addrs[i+1].c_str(), std::to_string(kStartPort+g_process_id).c_str(), &hints, &svr_cmd_dst[i]);
        if (ret) {
            SPDK_ERRLOG("getaddrinfo for dst failed\n");
            assert(false);
        }
        ret = rdma_resolve_addr(svr_cmd_cm_id_list[i], svr_cmd_src[i]->ai_addr, svr_cmd_dst[i]->ai_addr, 2000);
        if (ret) {
            SPDK_ERRLOG("resolve_addr failed\n");
            assert(false);
        }
        ret = rdma_get_cm_event(svr_cmd_cm_channel_list[i], &event);
        if(ret) {
            SPDK_ERRLOG("rdma_get_cm_event failed\n");
            assert(false);
        }
        if (event->event != RDMA_CM_EVENT_ADDR_RESOLVED) {
            SPDK_ERRLOG("event is not RDMA_CM_EVENT_ADDR_RESOLVED\n");
            assert(false);
        }
        rdma_ack_cm_event(event);

        ret = rdma_resolve_route(svr_cmd_cm_id_list[i], 2000);
        if (ret) {
            SPDK_ERRLOG("resolve_route failed\n");
            assert(false);
        }
        ret = rdma_get_cm_event(svr_cmd_cm_channel_list[i], &event);
        if(ret) {
            SPDK_ERRLOG("rdma_get_cm_event failed\n");
            assert(false);
        }
        if (event->event != RDMA_CM_EVENT_ROUTE_RESOLVED) {
            SPDK_ERRLOG("event is not RDMA_CM_EVENT_ROUTE_RESOLVED\n");
            assert(false);
        }
        rdma_ack_cm_event(event);

        svr_cmd_pd_list[i] = svr_cmd_cm_id_list[i]->pd;
        g_server_qp_list[i].cmd_cm_id = svr_cmd_cm_id_list[i];

        qp_attr.cap.max_send_wr = paraSend * g_num_qp;
        qp_attr.cap.max_send_sge = 1;
        qp_attr.cap.max_recv_wr = paraSend * g_num_qp;
        qp_attr.cap.max_recv_sge = 1;
        qp_attr.send_cq = g_cq;
        qp_attr.recv_cq = g_cq;

        qp_attr.qp_type = IBV_QPT_RC;
        ret = rdma_create_qp(svr_cmd_cm_id_list[i], svr_cmd_pd_list[i], &qp_attr);
        if(ret) {
            SPDK_ERRLOG("rdma_create_qp failed\n");
            assert(false);
        }


        g_server_qp_list[i].cmd_mr = ibv_reg_mr(svr_cmd_pd_list[i],
                                                g_server_qp_list[i].cmd_sendbuf,
                                                paraSend * g_num_qp * bufferSize * 2,
                                                IBV_ACCESS_LOCAL_WRITE);
        if (!g_server_qp_list[i].cmd_mr) {
            SPDK_ERRLOG("register memory failed\n");
            assert(false);
        }
        g_server_qp_list[i].mem_map = spdk_rdma_create_mem_map_external(svr_cmd_pd_list[i]);

        struct ibv_sge *send_sge, *recv_sge;
        struct send_wr_wrapper_server *send_wrapper;
        struct recv_wr_wrapper_server *recv_wrapper;
        struct ibv_recv_wr *recv_wr;
        TAILQ_FOREACH(send_wrapper, &g_server_qp_list[i].free_send, link) {
            send_sge = send_wrapper->send_wr.sg_list;
            send_sge->lkey = g_server_qp_list[i].cmd_mr->lkey;
        }
        for (recv_wr = g_server_qp_list[i].recv_wrs.first; recv_wr != NULL; recv_wr = recv_wr->next) {
            recv_sge = recv_wr->sg_list;
            recv_sge->lkey = g_server_qp_list[i].cmd_mr->lkey;
            recv_wrapper = (struct recv_wr_wrapper_server *) recv_wr->wr_id;
            recv_wrapper->bdev_context = allocate_bdev_context();
            if (!recv_wrapper->bdev_context) {
                SPDK_ERRLOG("failed to allocate recv_wrapper->bdev_context\n");
                assert(false);
            }
        }

        if (ibv_post_recv(g_server_qp_list[i].cmd_cm_id->qp, g_server_qp_list[i].recv_wrs.first, &bad_recv_wr)) {
            SPDK_ERRLOG("post send failed\n");
            assert(false);
        }
        g_server_qp_list[i].recv_wrs.first = NULL;

        ret = rdma_connect(svr_cmd_cm_id_list[i], &conn_param);
        if(ret) {
            SPDK_ERRLOG("rdma_connect failed\n");
            assert(false);
        }
    }

    std::ofstream outfile ("ready.log");

    outfile << "ready!" << std::endl;

    outfile.close();

    printf("accept connection\n");
    /* accept connection */
    // servers
    for (i = g_process_id+1; i < g_numOfServers; i++) {
        printf("accept connection %d\n", i);
        ret = rdma_get_cm_event(svr_cmd_cm_channel_list[i], &event);
        if(ret) {
            SPDK_ERRLOG("rdma_get_cm_event failed\n");
            assert(false);
        }
        if (event->event != RDMA_CM_EVENT_CONNECT_REQUEST) {
            SPDK_ERRLOG("event is not RDMA_CM_EVENT_CONNECT_REQUEST\n");
            assert(false);
        }

        g_server_qp_list[i].cmd_cm_id = event->id;
        rdma_ack_cm_event(event);

        svr_cmd_pd_list[i] = g_server_qp_list[i].cmd_cm_id->pd;

        qp_attr.cap.max_send_wr = paraSend * g_num_qp;
        qp_attr.cap.max_send_sge = 1;
        qp_attr.cap.max_recv_wr = paraSend * g_num_qp;
        qp_attr.cap.max_recv_sge = 1;
        qp_attr.send_cq = g_cq;
        qp_attr.recv_cq = g_cq;

        qp_attr.qp_type = IBV_QPT_RC;
        ret = rdma_create_qp(g_server_qp_list[i].cmd_cm_id, svr_cmd_pd_list[i], &qp_attr);
        if(ret) {
            SPDK_ERRLOG("rdma_create_qp failed\n");
            assert(false);
        }

        g_server_qp_list[i].cmd_mr = ibv_reg_mr(svr_cmd_pd_list[i],
                                                g_server_qp_list[i].cmd_sendbuf,
                                                paraSend * g_num_qp * bufferSize * 2,
                                                IBV_ACCESS_LOCAL_WRITE);
        if (!g_server_qp_list[i].cmd_mr) {
            SPDK_ERRLOG("register memory failed\n");
            assert(false);
        }
        g_server_qp_list[i].mem_map = spdk_rdma_create_mem_map_external(svr_cmd_pd_list[i]);

        struct ibv_sge *send_sge, *recv_sge;
        struct send_wr_wrapper_server *send_wrapper;
        struct recv_wr_wrapper_server *recv_wrapper;
        struct ibv_recv_wr *recv_wr;
        TAILQ_FOREACH(send_wrapper, &g_server_qp_list[i].free_send, link) {
            send_sge = send_wrapper->send_wr.sg_list;
            send_sge->lkey = g_server_qp_list[i].cmd_mr->lkey;
        }
        for (recv_wr = g_server_qp_list[i].recv_wrs.first; recv_wr != NULL; recv_wr = recv_wr->next) {
            recv_sge = recv_wr->sg_list;
            recv_sge->lkey = g_server_qp_list[i].cmd_mr->lkey;
            recv_wrapper = (struct recv_wr_wrapper_server *) recv_wr->wr_id;
            recv_wrapper->bdev_context = allocate_bdev_context();
            if (!recv_wrapper->bdev_context) {
                SPDK_ERRLOG("failed to allocate recv_wrapper->bdev_context\n");
                assert(false);
            }
        }

        if (ibv_post_recv(g_server_qp_list[i].cmd_cm_id->qp, g_server_qp_list[i].recv_wrs.first, &bad_recv_wr)) {
            SPDK_ERRLOG("post send failed\n");
            assert(false);
        }
        g_server_qp_list[i].recv_wrs.first = NULL;

        ret = rdma_accept(g_server_qp_list[i].cmd_cm_id, &conn_param);
        if(ret) {
            SPDK_ERRLOG("rdma_accept failed\n");
            assert(false);
        }
    }

    printf("check connection\n");

    /* check connection request */
    // servers
    for (i = 0; i < g_numOfServers; i++) {
        if (i == g_process_id) {
            continue;
        }
        printf("check connection %d\n", i);
        ret = rdma_get_cm_event(svr_cmd_cm_channel_list[i], &event);
        if(ret) {
            SPDK_ERRLOG("rdma_get_cm_event failed\n");
            assert(false);
        }
        if (event->event != RDMA_CM_EVENT_ESTABLISHED) {
            SPDK_ERRLOG("event is not RDMA_CM_EVENT_ESTABLISHED, but %d\n", event->event);
            assert(false);
        }
        rdma_ack_cm_event(event);
    }

    /* accept connection */
    // host
    for (i = 0; i < g_num_qp; i++) {
        ret = rdma_get_cm_event(host_cmd_cm_channel_list[i], &event);
        if(ret) {
            SPDK_ERRLOG("rdma_get_cm_event failed\n");
            assert(false);
        }
        if (event->event != RDMA_CM_EVENT_CONNECT_REQUEST) {
            SPDK_ERRLOG("event is not RDMA_CM_EVENT_CONNECT_REQUEST, but %d\n", event->event);
            assert(false);
        }
        g_host_qp_list[i].cmd_cm_id = event->id;
        rdma_ack_cm_event(event);

        host_cmd_pd_list[i] = g_host_qp_list[i].cmd_cm_id->pd;

        qp_attr.cap.max_send_wr = spdk_min(paraSend * 17, 4096);
        qp_attr.cap.max_send_sge = 1;
        qp_attr.cap.max_recv_wr = paraSend;
        qp_attr.cap.max_recv_sge = 1;
        qp_attr.send_cq = g_cq;
        qp_attr.recv_cq = g_cq;

        qp_attr.qp_type = IBV_QPT_RC;
        ret = rdma_create_qp(g_host_qp_list[i].cmd_cm_id, host_cmd_pd_list[i], &qp_attr);
        if(ret) {
            SPDK_ERRLOG("rdma_create_qp failed\n");
            assert(false);
        }

        g_host_qp_list[i].cmd_mr = ibv_reg_mr(host_cmd_pd_list[i],
                                              g_host_qp_list[i].cmd_sendbuf,
                                              paraSend * bufferSize * 2,
                                              IBV_ACCESS_LOCAL_WRITE);
        if (!g_host_qp_list[i].cmd_mr) {
            SPDK_ERRLOG("register memory failed\n");
            assert(false);
        }
        g_host_qp_list[i].mem_map = spdk_rdma_create_mem_map_external(host_cmd_pd_list[i]);

        struct ibv_sge *send_sge, *recv_sge;
        struct send_wr_wrapper_server *send_wrapper;
        struct recv_wr_wrapper_server *recv_wrapper;
        struct ibv_recv_wr *recv_wr;
        TAILQ_FOREACH(send_wrapper, &g_host_qp_list[i].free_send, link) {
            send_sge = send_wrapper->send_wr.sg_list;
            send_sge->lkey = g_host_qp_list[i].cmd_mr->lkey;
        }
        for (recv_wr = g_host_qp_list[i].recv_wrs.first; recv_wr != NULL; recv_wr = recv_wr->next) {
            recv_sge = recv_wr->sg_list;
            recv_sge->lkey = g_host_qp_list[i].cmd_mr->lkey;
            recv_wrapper = (struct recv_wr_wrapper_server *) recv_wr->wr_id;
            recv_wrapper->bdev_context = allocate_bdev_context();
            if (!recv_wrapper->bdev_context) {
                SPDK_ERRLOG("failed to allocate recv_wrapper->bdev_context\n");
                assert(false);
            }
        }

        if (ibv_post_recv(g_host_qp_list[i].cmd_cm_id->qp, g_host_qp_list[i].recv_wrs.first, &bad_recv_wr)) {
            SPDK_ERRLOG("post send failed\n");
            assert(false);
        }
        g_host_qp_list[i].recv_wrs.first = NULL;

        ret = rdma_accept(g_host_qp_list[i].cmd_cm_id, &conn_param);
        if(ret) {
            SPDK_ERRLOG("rdma_accept failed\n");
            assert(false);
        }
    }

    /* check connection request */
    // host
    for (i = 0; i < g_num_qp; i++) {
        ret = rdma_get_cm_event(host_cmd_cm_channel_list[i], &event);
        if(ret) {
            SPDK_ERRLOG("rdma_get_cm_event failed\n");
            assert(false);
        }
        if (event->event != RDMA_CM_EVENT_ESTABLISHED) {
            SPDK_ERRLOG("event is not RDMA_CM_EVENT_ESTABLISHED, but %d\n", event->event);
            assert(false);
        }
        rdma_ack_cm_event(event);
    }
}

void
bdev_start(void *arg1)
{
    struct bdev_context_t *bdev_context = (struct bdev_context_t *) arg1;
    int rc = 0;
    struct rte_hash_parameters hash_params = {0};
    char name_buf[32];
    hash_params.entries = paraSend * g_num_qp;
    hash_params.key_len = sizeof(uint64_t);

    bdev_context->bdev_thread = spdk_get_thread();

    /*
     * There can be many bdevs configured, but this application will only use
     * the one input by the user at runtime.
     *
     * Open the bdev by calling spdk_bdev_open_ext() with its name.
     * The function will return a descriptor
     */
    rc = spdk_bdev_open_ext(bdev_context->bdev_name, true, bdev_event_cb, NULL,
                            &bdev_context->bdev_desc);
    if (rc) {
        SPDK_ERRLOG("Could not open bdev: %s\n", bdev_context->bdev_name);
        spdk_app_stop(-1);
        return;
    }

    /* A bdev pointer is valid while the bdev is opened. */
    bdev_context->bdev = spdk_bdev_desc_get_bdev(bdev_context->bdev_desc);

    /* Open I/O channel */
    bdev_context->bdev_io_channel = spdk_bdev_get_io_channel(bdev_context->bdev_desc);
    if (bdev_context->bdev_io_channel == NULL) {
        SPDK_ERRLOG("Could not create bdev I/O channel!!\n");
        spdk_bdev_close(bdev_context->bdev_desc);
        spdk_app_stop(-1);
        return;
    }

    bdev_context->blk_size = spdk_bdev_get_block_size(bdev_context->bdev);
    bdev_context->blk_size_shift = spdk_u32log2(bdev_context->blk_size);
    bdev_context->blockcnt = bdev_context->bdev->blockcnt;
    bdev_context->buf_align = spdk_max(spdk_bdev_get_buf_align(bdev_context->bdev), 32);
    g_strip_size = g_strip_size * 1024 / bdev_context->blk_size;

    snprintf(name_buf, sizeof(name_buf), "raidx_hash_req");
    hash_params.name = name_buf;
    g_req_hash = rte_hash_create(&hash_params);

    snprintf(name_buf, sizeof(name_buf), "raidx_hash_rc");
    hash_params.name = name_buf;
    hash_params.key_len = sizeof(struct raidx_rc_hash_key);
    g_rc_hash = rte_hash_create(&hash_params);

    for (int coeff = 0; coeff < 256; ++coeff) {
        gf_vect_mul_init((unsigned char)coeff, g_raidx_gf_tbl[coeff]);
    }

    connect_rdma_connections();
    g_poller = SPDK_POLLER_REGISTER(poll_cq, NULL, 0);
}

int
main(int argc, char **argv)
{
    int rc = 0;

    /* SPDK initialization */
    g_bdev_context = (struct bdev_context_t *) calloc(1, sizeof(struct bdev_context_t));
    struct spdk_app_opts opts = {};

    /* Set default values in opts structure. */
    spdk_app_opts_init(&opts, sizeof(opts));
    opts.name = "rpc_bdev";

    /*
     *  Parse built-in SPDK command line parameters as well
     *  as our custom one(s).
     */
    if ((rc = spdk_app_parse_args(argc, argv, &opts, "b:P:a:S:E:N:", NULL, bdev_parse_arg,
                                  bdev_usage)) != SPDK_APP_PARSE_ARGS_SUCCESS) {
        exit(rc);
    }
    g_bdev_context->bdev_name = g_bdev_name;

    std::ifstream addrs(g_addr_file, std::ios::in);
    std::string ip_addr;
    while(addrs>>ip_addr) {
        ip_addrs.push_back(ip_addr);
    }
    addrs.close();

    /*
     * spdk_app_start() will initialize the SPDK framework, call bdev_start(),
     * and then block until spdk_app_stop() is called (or if an initialization
     * error occurs, spdk_app_start() will return with rc even without calling
     * hello_start().
     */
    rc = spdk_app_start(&opts, bdev_start, g_bdev_context);
    if (rc) {
        SPDK_ERRLOG("ERROR starting application\n");
    }

    /* At this point either spdk_app_stop() was called, or spdk_app_start()
     * failed because of internal error.
     */

    /* Gracefully close out all of the SPDK subsystems. */
    spdk_app_fini();

    free(g_bdev_context);
}

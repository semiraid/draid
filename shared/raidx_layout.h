#ifndef BDEV_RAID_RAIDX_LAYOUT_H
#define BDEV_RAID_RAIDX_LAYOUT_H

#include "common.h"

static inline bool
raidx_validate_geometry(uint8_t total_chunks, uint8_t num_parities)
{
    return num_parities >= 1 &&
           num_parities <= kRaidxMaxParityRows &&
           total_chunks > num_parities;
}

static inline uint8_t
raidx_num_data_chunks(uint8_t total_chunks, uint8_t num_parities)
{
    return total_chunks - num_parities;
}

static inline uint8_t
raidx_parity_start(uint64_t stripe_index, uint8_t total_chunks)
{
    return (uint8_t)((total_chunks - 1 - (stripe_index % total_chunks) + total_chunks) % total_chunks);
}

static inline uint8_t
raidx_parity_slot(uint64_t stripe_index, uint8_t total_chunks, uint8_t parity_row)
{
    return (uint8_t)((raidx_parity_start(stripe_index, total_chunks) + parity_row) % total_chunks);
}

static inline bool
raidx_is_parity_slot(uint64_t stripe_index, uint8_t total_chunks, uint8_t num_parities, uint8_t slot_index)
{
    for (uint8_t row = 0; row < num_parities; ++row) {
        if (raidx_parity_slot(stripe_index, total_chunks, row) == slot_index) {
            return true;
        }
    }

    return false;
}

static inline int8_t
raidx_chunk_data_index(uint64_t stripe_index, uint8_t total_chunks, uint8_t num_parities, uint8_t slot_index)
{
    uint8_t data_index = 0;

    for (uint8_t slot = 0; slot < total_chunks; ++slot) {
        if (raidx_is_parity_slot(stripe_index, total_chunks, num_parities, slot)) {
            continue;
        }

        if (slot == slot_index) {
            return (int8_t)data_index;
        }

        ++data_index;
    }

    return -1;
}

static inline int8_t
raidx_data_slot(uint64_t stripe_index, uint8_t total_chunks, uint8_t num_parities, uint8_t data_index)
{
    uint8_t current_data_index = 0;

    for (uint8_t slot = 0; slot < total_chunks; ++slot) {
        if (raidx_is_parity_slot(stripe_index, total_chunks, num_parities, slot)) {
            continue;
        }

        if (current_data_index == data_index) {
            return (int8_t)slot;
        }

        ++current_data_index;
    }

    return -1;
}

static inline int8_t
raidx_slot_parity_row(uint64_t stripe_index, uint8_t total_chunks, uint8_t num_parities, uint8_t slot_index)
{
    for (uint8_t row = 0; row < num_parities; ++row) {
        if (raidx_parity_slot(stripe_index, total_chunks, row) == slot_index) {
            return (int8_t)row;
        }
    }

    return -1;
}

#endif

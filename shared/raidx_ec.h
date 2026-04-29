#ifndef BDEV_RAID_RAIDX_EC_H
#define BDEV_RAID_RAIDX_EC_H

#include "common.h"
#include "raidx_layout.h"

struct raidx_ec_engine {
    uint8_t num_data_chunks;
    uint8_t num_parities;
    unsigned char *encode_matrix;
    unsigned char *encode_tables;
};

static inline unsigned char
raidx_gf_pow(unsigned char base, unsigned int exp)
{
    unsigned char value = 1;

    while (exp-- > 0) {
        value = gf_mul(value, base);
    }

    return value;
}

static inline unsigned char
raidx_parity_coeff(uint8_t parity_row, uint8_t data_index)
{
    if (parity_row == 0) {
        return 1;
    }

    return raidx_gf_pow(2, parity_row * data_index);
}

static inline int
raidx_ec_init(struct raidx_ec_engine *engine, uint8_t num_data_chunks, uint8_t num_parities)
{
    size_t matrix_len;
    size_t tables_len;

    if (!raidx_validate_geometry((uint8_t)(num_data_chunks + num_parities), num_parities)) {
        return -EINVAL;
    }

    memset(engine, 0, sizeof(*engine));
    engine->num_data_chunks = num_data_chunks;
    engine->num_parities = num_parities;

    matrix_len = (size_t)num_data_chunks * num_parities;
    tables_len = matrix_len * 32;

    engine->encode_matrix = (unsigned char *)calloc(matrix_len, sizeof(unsigned char));
    engine->encode_tables = (unsigned char *)calloc(tables_len, sizeof(unsigned char));
    if (engine->encode_matrix == NULL || engine->encode_tables == NULL) {
        free(engine->encode_matrix);
        free(engine->encode_tables);
        memset(engine, 0, sizeof(*engine));
        return -ENOMEM;
    }

    for (uint8_t row = 0; row < num_parities; ++row) {
        for (uint8_t col = 0; col < num_data_chunks; ++col) {
            engine->encode_matrix[row * num_data_chunks + col] = raidx_parity_coeff(row, col);
        }
    }

    ec_init_tables(num_data_chunks, num_parities, engine->encode_matrix, engine->encode_tables);
    return 0;
}

static inline void
raidx_ec_fini(struct raidx_ec_engine *engine)
{
    free(engine->encode_matrix);
    free(engine->encode_tables);
    memset(engine, 0, sizeof(*engine));
}

static inline void
raidx_ec_encode_full_stripe(const struct raidx_ec_engine *engine, size_t chunk_len,
                            unsigned char **data_ptrs, unsigned char **parity_ptrs)
{
    ec_encode_data((int)chunk_len, engine->num_data_chunks, engine->num_parities,
                   engine->encode_tables, data_ptrs, parity_ptrs);
}

#endif

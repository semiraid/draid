#include "spdk/stdinc.h"
#include "spdk/thread.h"
#include "spdk/bdev.h"
#include "spdk/env.h"
#include "spdk/event.h"
#include "spdk/log.h"
#include "spdk/string.h"
#include "spdk/bdev_zone.h"
#include "bdev_raid.h"

static char *g_bdev_name = (char*) "Raid0";
static uint64_t start_offset = 1;
static uint64_t span_length = 5;
static uint64_t g_repeat_count = 1;
static uint64_t g_stride_blocks = 0;
static uint64_t g_final_read_offset = 0;
static uint64_t g_final_read_blocks = 0;
static bool g_final_read_offset_set = false;
static uint64_t g_prefill_offset = 0;
static uint64_t g_prefill_blocks = 0;
static bool g_prefill_offset_set = false;
static unsigned int g_data_seed = 1;
static uint8_t g_degrade_after_write_slots[64];
static uint8_t g_degrade_after_write_slot_count = 0;

static int
read_env_u64(const char *name, uint64_t *value, bool allow_zero)
{
	const char *env_value = getenv(name);
	char *end_ptr = NULL;
	unsigned long long parsed_value;

	if (env_value == NULL || env_value[0] == '\0') {
		return 0;
	}

	errno = 0;
	parsed_value = strtoull(env_value, &end_ptr, 0);
	if (errno != 0 || end_ptr == env_value || *end_ptr != '\0' ||
	    (!allow_zero && parsed_value == 0)) {
		fprintf(stderr, "Invalid %s=%s\n", name, env_value);
		return -EINVAL;
	}

	*value = parsed_value;
	return 0;
}

static int
read_env_u32(const char *name, unsigned int *value)
{
	const char *env_value = getenv(name);
	uint64_t parsed_value;
	int rc;

	if (env_value == NULL || env_value[0] == '\0') {
		return 0;
	}

	rc = read_env_u64(name, &parsed_value, true);

	if (rc != 0) {
		return rc;
	}

	if (parsed_value > UINT_MAX) {
		fprintf(stderr, "Invalid %s=%" PRIu64 "\n", name, parsed_value);
		return -EINVAL;
	}

	*value = (unsigned int)parsed_value;
	return 0;
}

static int
read_env_slot_list(const char *name, uint8_t *slots, uint8_t *slot_count, uint8_t max_slots)
{
	const char *env_value = getenv(name);
	char *copy;
	char *save_ptr = NULL;
	char *token;

	*slot_count = 0;
	if (env_value == NULL || env_value[0] == '\0') {
		return 0;
	}

	copy = strdup(env_value);
	if (copy == NULL) {
		return -ENOMEM;
	}

	for (token = strtok_r(copy, ",", &save_ptr);
	     token != NULL;
	     token = strtok_r(NULL, ",", &save_ptr)) {
		char *end_ptr = NULL;
		unsigned long parsed_value;

		if (*slot_count >= max_slots) {
			fprintf(stderr, "Too many values for %s\n", name);
			free(copy);
			return -EINVAL;
		}

		errno = 0;
		parsed_value = strtoul(token, &end_ptr, 0);
		if (errno != 0 || end_ptr == token || *end_ptr != '\0' ||
		    parsed_value > UINT8_MAX) {
			fprintf(stderr, "Invalid %s=%s\n", name, env_value);
			free(copy);
			return -EINVAL;
		}

		slots[*slot_count] = (uint8_t)parsed_value;
		(*slot_count)++;
	}

	free(copy);
	return 0;
}

static int
load_test_parameters_from_env(void)
{
	int rc;
	const char *env_value;

	rc = read_env_u64("DRAID_TEST_OFFSET_BLOCKS", &start_offset, true);
	if (rc != 0) {
		return rc;
	}

	rc = read_env_u64("DRAID_TEST_LENGTH_BLOCKS", &span_length, false);
	if (rc != 0) {
		return rc;
	}

	rc = read_env_u32("DRAID_TEST_SEED", &g_data_seed);
	if (rc != 0) {
		return rc;
	}

	rc = read_env_u64("DRAID_TEST_REPEAT_COUNT", &g_repeat_count, false);
	if (rc != 0) {
		return rc;
	}

	rc = read_env_u64("DRAID_TEST_STRIDE_BLOCKS", &g_stride_blocks, true);
	if (rc != 0) {
		return rc;
	}

	env_value = getenv("DRAID_TEST_FINAL_READ_OFFSET_BLOCKS");
	if (env_value != NULL && env_value[0] != '\0') {
		rc = read_env_u64("DRAID_TEST_FINAL_READ_OFFSET_BLOCKS", &g_final_read_offset, true);
		if (rc != 0) {
			return rc;
		}
		g_final_read_offset_set = true;
	}

	rc = read_env_u64("DRAID_TEST_FINAL_READ_LENGTH_BLOCKS", &g_final_read_blocks, true);
	if (rc != 0) {
		return rc;
	}

	env_value = getenv("DRAID_TEST_PREFILL_OFFSET_BLOCKS");
	if (env_value != NULL && env_value[0] != '\0') {
		rc = read_env_u64("DRAID_TEST_PREFILL_OFFSET_BLOCKS", &g_prefill_offset, true);
		if (rc != 0) {
			return rc;
		}
		g_prefill_offset_set = true;
	}

	rc = read_env_u64("DRAID_TEST_PREFILL_LENGTH_BLOCKS", &g_prefill_blocks, true);
	if (rc != 0) {
		return rc;
	}

	rc = read_env_slot_list("DRAID_TEST_DEGRADE_AFTER_WRITE_SLOTS",
				g_degrade_after_write_slots,
				&g_degrade_after_write_slot_count,
				(uint8_t)(sizeof(g_degrade_after_write_slots) /
					  sizeof(g_degrade_after_write_slots[0])));
	if (rc != 0) {
		return rc;
	}

	return 0;
}

/*
 * We'll use this struct to gather housekeeping hello_context to pass between
 * our events and callbacks.
 */
struct hello_context_t {
	struct spdk_bdev *bdev;
	struct spdk_bdev_desc *bdev_desc;
	struct spdk_io_channel *bdev_io_channel;
	char *buff;
    char *init_buff;
	char *final_expected_buff;
	char *bdev_name;
	struct spdk_bdev_io_wait_entry bdev_io_wait;
    size_t buff_len;
	size_t op_buff_len;
	size_t final_buff_len;
	size_t prefill_buff_len;
	uint32_t blk_size;
	uint64_t current_iteration;
	uint64_t repeat_count;
	uint64_t stride_blocks;
	uint64_t current_offset;
	uint64_t current_io_blocks;
	uint64_t final_read_offset;
	uint64_t final_read_blocks;
	uint64_t prefill_offset;
	uint64_t prefill_blocks;
	uint64_t write_start_ns;
	uint64_t read_start_ns;
	uint64_t total_write_ns;
	uint64_t total_read_ns;
	uint64_t total_final_read_ns;
	bool reading_final;
	bool prefilling;
	bool post_write_degradation_applied;
};

void rand_buffer(unsigned char *buf, long buffer_size);
static void hello_read(void *arg);
static void hello_write(void *arg);

static uint64_t
get_monotonic_ns(void)
{
	struct timespec ts;

	clock_gettime(CLOCK_MONOTONIC, &ts);
	return (uint64_t)ts.tv_sec * 1000000000ULL + ts.tv_nsec;
}

static void
prepare_write_buffer(struct hello_context_t *hello_context)
{
	uint64_t write_start;
	uint64_t write_end;
	uint64_t final_start;
	uint64_t final_end;
	uint64_t overlap_start;
	uint64_t overlap_end;
	size_t write_len;
	size_t src_offset;
	size_t dst_offset;
	size_t copy_len;

	write_len = hello_context->current_io_blocks * hello_context->blk_size;
	rand_buffer((unsigned char*)hello_context->init_buff, write_len);
	memcpy(hello_context->buff, hello_context->init_buff, write_len);

	if (hello_context->final_read_blocks == 0) {
		return;
	}

	write_start = hello_context->current_offset;
	write_end = write_start + hello_context->current_io_blocks;
	final_start = hello_context->final_read_offset;
	final_end = final_start + hello_context->final_read_blocks;

	overlap_start = write_start > final_start ? write_start : final_start;
	overlap_end = write_end < final_end ? write_end : final_end;

	if (overlap_start >= overlap_end) {
		return;
	}

	src_offset = (overlap_start - write_start) * hello_context->blk_size;
	dst_offset = (overlap_start - final_start) * hello_context->blk_size;
	copy_len = (overlap_end - overlap_start) * hello_context->blk_size;
	memcpy(hello_context->final_expected_buff + dst_offset,
	       hello_context->init_buff + src_offset,
	       copy_len);
}

static void
print_perf_result(struct hello_context_t *hello_context)
{
	double write_sec = (double)hello_context->total_write_ns / 1000000000.0;
	double read_sec = (double)hello_context->total_read_ns / 1000000000.0;
	double final_read_sec = (double)hello_context->total_final_read_ns / 1000000000.0;
	double total_mib = ((double)hello_context->op_buff_len * hello_context->repeat_count) /
		(1024.0 * 1024.0);
	double final_mib = (double)hello_context->final_buff_len / (1024.0 * 1024.0);
	double write_iops = write_sec > 0.0 ? (double)hello_context->repeat_count / write_sec : 0.0;
	double read_iops = read_sec > 0.0 ? (double)hello_context->repeat_count / read_sec : 0.0;
	double write_mib_s = write_sec > 0.0 ? total_mib / write_sec : 0.0;
	double read_mib_s = read_sec > 0.0 ? total_mib / read_sec : 0.0;
	double final_read_mib_s = final_read_sec > 0.0 ? final_mib / final_read_sec : 0.0;

	SPDK_NOTICELOG("PERF_RESULT repeat_count=%llu length_blocks=%llu bytes_per_op=%zu "
		       "final_read_blocks=%llu total_write_sec=%.9f total_read_sec=%.9f "
		       "total_final_read_sec=%.9f write_iops=%.3f read_iops=%.3f "
		       "write_mib_s=%.3f read_mib_s=%.3f final_read_mib_s=%.3f\n",
		       (unsigned long long)hello_context->repeat_count,
		       (unsigned long long)span_length,
		       hello_context->op_buff_len,
		       (unsigned long long)hello_context->final_read_blocks,
		       write_sec,
		       read_sec,
		       final_read_sec,
		       write_iops,
		       read_iops,
		       write_mib_s,
		       read_mib_s,
		       final_read_mib_s);
}

static int
apply_post_write_degradation(struct hello_context_t *hello_context)
{
	struct raid_bdev_config *raid_cfg;
	struct raid_bdev *raid_bdev;

	if (g_degrade_after_write_slot_count == 0 ||
	    hello_context->post_write_degradation_applied) {
		return 0;
	}

	raid_cfg = raid_bdev_config_find_by_name(hello_context->bdev_name);
	if (raid_cfg == NULL || raid_cfg->raid_bdev == NULL) {
		SPDK_ERRLOG("Unable to find raid bdev for post-write degradation: %s\n",
			    hello_context->bdev_name);
		return -ENOENT;
	}

	raid_bdev = raid_cfg->raid_bdev;
	for (uint8_t i = 0; i < g_degrade_after_write_slot_count; ++i) {
		uint8_t slot = g_degrade_after_write_slots[i];

		if (slot >= raid_bdev->num_base_rpcs) {
			SPDK_ERRLOG("Invalid post-write degraded slot %u for %u base RPCs\n",
				    slot, raid_bdev->num_base_rpcs);
			return -EINVAL;
		}

		raid_bdev->base_rpc_info[slot].degraded = true;
		raid_bdev->degraded = true;
		SPDK_NOTICELOG("DRAID_TEST_DEGRADED_AFTER_WRITE slot=%u\n", slot);
	}

	hello_context->post_write_degradation_applied = true;
	return 0;
}

static void
stop_hello_context(struct hello_context_t *hello_context, int status)
{
	spdk_put_io_channel(hello_context->bdev_io_channel);
	spdk_bdev_close(hello_context->bdev_desc);
	SPDK_NOTICELOG("Stopping app\n");
	spdk_app_stop(status);
}

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

void rand_buffer(unsigned char *buf, long buffer_size)
{
    long i;
    for (i = 0; i < buffer_size; i++)
        buf[i] = rand();
}

/*
 * Usage function for printing parameters that are specific to this application
 */
static void
hello_bdev_usage(void)
{
	printf(" -b <bdev>                 name of the bdev to use\n");
}

/*
 * This function is called to parse the parameters that are specific to this application
 */
static int hello_bdev_parse_arg(int ch, char *arg)
{
	switch (ch) {
	case 'b':
		g_bdev_name = arg;
		break;
	default:
		return -EINVAL;
	}
	return 0;
}

/*
 * Callback function for read io completion.
 */
static void
read_complete(struct spdk_bdev_io *bdev_io, bool success, void *cb_arg)
{
	struct hello_context_t *hello_context = (struct hello_context_t*) cb_arg;
	char *expected_buff = hello_context->reading_final ?
		hello_context->final_expected_buff : hello_context->init_buff;
	size_t compare_len = hello_context->reading_final ?
		hello_context->final_buff_len : hello_context->op_buff_len;
	bool data_matches = false;
	uint64_t elapsed_ns;

	elapsed_ns = get_monotonic_ns() - hello_context->read_start_ns;
	if (hello_context->reading_final) {
		hello_context->total_final_read_ns += elapsed_ns;
	} else {
		hello_context->total_read_ns += elapsed_ns;
	}

	if (success) {
		SPDK_NOTICELOG("Read string from bdev\n");
		int ret = memcmp(hello_context->buff, expected_buff, compare_len);
        if (ret) {
            SPDK_ERRLOG("read does not match write\n");
			for (size_t i = 0; i < compare_len; i++) {
                if (hello_context->buff[i] != expected_buff[i]) {
                    SPDK_ERRLOG("at index %zu, init %c, return %c\n",
                                i, expected_buff[i], hello_context->buff[i]);
                    show_buffer((unsigned char*)hello_context->buff, compare_len);
                    break;
                }
            }
        } else {
            SPDK_NOTICELOG("bdev io read completed successfully\n");
            data_matches = true;
        }
	} else {
		SPDK_ERRLOG("bdev io read error\n");
	}

	/* Complete the bdev io and close the channel */
	spdk_bdev_free_io(bdev_io);
	if (hello_context->reading_final) {
		if (success && data_matches) {
			print_perf_result(hello_context);
		}
		stop_hello_context(hello_context, success && data_matches ? 0 : -1);
		return;
	}

	if (success && data_matches &&
	    hello_context->current_iteration + 1 < hello_context->repeat_count) {
		hello_context->current_iteration++;
		hello_context->current_offset = start_offset +
			hello_context->current_iteration * hello_context->stride_blocks;
		prepare_write_buffer(hello_context);
		hello_write(hello_context);
		return;
	}

	if (success && data_matches && hello_context->final_read_blocks > 0) {
		hello_context->reading_final = true;
		hello_context->current_offset = hello_context->final_read_offset;
		hello_read(hello_context);
		return;
	}

	if (success && data_matches) {
		print_perf_result(hello_context);
	}

	stop_hello_context(hello_context, success && data_matches ? 0 : -1);
}

static void
hello_read(void *arg)
{
	struct hello_context_t *hello_context = (struct hello_context_t*) arg;
	int rc = 0;

	SPDK_NOTICELOG("Reading io\n");
	hello_context->current_io_blocks = hello_context->reading_final ?
		hello_context->final_read_blocks : span_length;
	memset(hello_context->buff, 0,
	       hello_context->reading_final ? hello_context->final_buff_len :
	       hello_context->op_buff_len);
	hello_context->read_start_ns = get_monotonic_ns();
	rc = spdk_bdev_read_blocks(hello_context->bdev_desc, hello_context->bdev_io_channel,
			    hello_context->buff, hello_context->current_offset,
			    hello_context->current_io_blocks,
			    read_complete, hello_context);

	if (rc == -ENOMEM) {
		SPDK_NOTICELOG("Queueing io\n");
		/* In case we cannot perform I/O now, queue I/O */
		hello_context->bdev_io_wait.bdev = hello_context->bdev;
		hello_context->bdev_io_wait.cb_fn = hello_read;
		hello_context->bdev_io_wait.cb_arg = hello_context;
		spdk_bdev_queue_io_wait(hello_context->bdev, hello_context->bdev_io_channel,
					&hello_context->bdev_io_wait);
	} else if (rc) {
		SPDK_ERRLOG("%s error while reading from bdev: %d\n", spdk_strerror(-rc), rc);
		stop_hello_context(hello_context, -1);
	}
}

/*
 * Callback function for write io completion.
 */
static void
write_complete(struct spdk_bdev_io *bdev_io, bool success, void *cb_arg)
{
	struct hello_context_t *hello_context = (struct hello_context_t*) cb_arg;
	size_t length;

	/* Complete the I/O */
	spdk_bdev_free_io(bdev_io);
	hello_context->total_write_ns += get_monotonic_ns() - hello_context->write_start_ns;

	if (success) {
		SPDK_NOTICELOG("bdev io write completed successfully\n");
	} else {
		SPDK_ERRLOG("bdev io write error: %d\n", EIO);
		stop_hello_context(hello_context, -1);
		return;
	}

	if (apply_post_write_degradation(hello_context) != 0) {
		stop_hello_context(hello_context, -1);
		return;
	}

	if (hello_context->prefilling) {
		hello_context->prefilling = false;
		hello_context->current_iteration = 0;
		hello_context->current_offset = start_offset;
		hello_context->current_io_blocks = span_length;
		prepare_write_buffer(hello_context);
		hello_write(hello_context);
		return;
	}

	/* Zero the buffer so that we can use it for reading */
	length = hello_context->current_io_blocks * hello_context->blk_size;
	memset(hello_context->buff, 0, length);

	hello_read(hello_context);
}

static void
hello_write(void *arg)
{
	struct hello_context_t *hello_context = (struct hello_context_t*) arg;
	int rc = 0;

	SPDK_NOTICELOG("Writing to the bdev\n");
	hello_context->reading_final = false;
	if (hello_context->current_io_blocks == 0) {
		hello_context->current_io_blocks = span_length;
	}
	hello_context->write_start_ns = get_monotonic_ns();
	rc = spdk_bdev_write_blocks(hello_context->bdev_desc, hello_context->bdev_io_channel,
			     hello_context->buff, hello_context->current_offset,
			     hello_context->current_io_blocks,
			     write_complete, hello_context);

	if (rc == -ENOMEM) {
		SPDK_NOTICELOG("Queueing io\n");
		/* In case we cannot perform I/O now, queue I/O */
		hello_context->bdev_io_wait.bdev = hello_context->bdev;
		hello_context->bdev_io_wait.cb_fn = hello_write;
		hello_context->bdev_io_wait.cb_arg = hello_context;
		spdk_bdev_queue_io_wait(hello_context->bdev, hello_context->bdev_io_channel,
					&hello_context->bdev_io_wait);
	} else if (rc) {
		SPDK_ERRLOG("%s error while writing to bdev: %d\n", spdk_strerror(-rc), rc);
		stop_hello_context(hello_context, -1);
	}
}

static void
hello_bdev_event_cb(enum spdk_bdev_event_type type, struct spdk_bdev *bdev,
		    void *event_ctx)
{
	SPDK_NOTICELOG("Unsupported bdev event: type %d\n", type);
}

static void
reset_zone_complete(struct spdk_bdev_io *bdev_io, bool success, void *cb_arg)
{
	struct hello_context_t *hello_context = (struct hello_context_t*) cb_arg;

	/* Complete the I/O */
	spdk_bdev_free_io(bdev_io);

	if (!success) {
		SPDK_ERRLOG("bdev io reset zone error: %d\n", EIO);
		spdk_put_io_channel(hello_context->bdev_io_channel);
		spdk_bdev_close(hello_context->bdev_desc);
		spdk_app_stop(-1);
		return;
	}

	hello_write(hello_context);
}

static void
hello_reset_zone(void *arg)
{
	struct hello_context_t *hello_context = (struct hello_context_t*) arg;
	int rc = 0;

	rc = spdk_bdev_zone_management(hello_context->bdev_desc, hello_context->bdev_io_channel,
				       0, SPDK_BDEV_ZONE_RESET, reset_zone_complete, hello_context);

	if (rc == -ENOMEM) {
		SPDK_NOTICELOG("Queueing io\n");
		/* In case we cannot perform I/O now, queue I/O */
		hello_context->bdev_io_wait.bdev = hello_context->bdev;
		hello_context->bdev_io_wait.cb_fn = hello_reset_zone;
		hello_context->bdev_io_wait.cb_arg = hello_context;
		spdk_bdev_queue_io_wait(hello_context->bdev, hello_context->bdev_io_channel,
					&hello_context->bdev_io_wait);
	} else if (rc) {
		SPDK_ERRLOG("%s error while resetting zone: %d\n", spdk_strerror(-rc), rc);
		spdk_put_io_channel(hello_context->bdev_io_channel);
		spdk_bdev_close(hello_context->bdev_desc);
		spdk_app_stop(-1);
	}
}

/*
 * Our initial event that kicks off everything from main().
 */
static void
hello_start(void *arg1)
{
	struct hello_context_t *hello_context = (struct hello_context_t*) arg1;
	uint32_t blk_size, buf_align;
	uint64_t num_blocks;
	uint64_t last_io_end;
	uint64_t prefill_end;
	int rc = 0;
	hello_context->bdev = NULL;
	hello_context->bdev_desc = NULL;

	SPDK_NOTICELOG("Successfully started the application\n");

	/*
	 * There can be many bdevs configured, but this application will only use
	 * the one input by the user at runtime.
	 *
	 * Open the bdev by calling spdk_bdev_open_ext() with its name.
	 * The function will return a descriptor
	 */
	SPDK_NOTICELOG("Opening the bdev %s\n", hello_context->bdev_name);
	rc = spdk_bdev_open_ext(hello_context->bdev_name, true, hello_bdev_event_cb, NULL,
				&hello_context->bdev_desc);
	if (rc) {
		SPDK_ERRLOG("Could not open bdev: %s\n", hello_context->bdev_name);
		spdk_app_stop(-1);
		return;
	}

	/* A bdev pointer is valid while the bdev is opened. */
	hello_context->bdev = spdk_bdev_desc_get_bdev(hello_context->bdev_desc);

	hello_context->repeat_count = g_repeat_count;
	hello_context->stride_blocks = g_stride_blocks == 0 ? span_length : g_stride_blocks;
	hello_context->current_iteration = 0;
	hello_context->current_offset = start_offset;
	hello_context->current_io_blocks = span_length;
	hello_context->final_read_blocks = g_final_read_blocks;
	hello_context->final_read_offset = g_final_read_offset_set ? g_final_read_offset : start_offset;
	hello_context->prefill_blocks = g_prefill_blocks;
	hello_context->prefill_offset = g_prefill_offset_set ? g_prefill_offset : start_offset;
	hello_context->reading_final = false;
	hello_context->prefilling = false;
	hello_context->post_write_degradation_applied = false;
	hello_context->total_write_ns = 0;
	hello_context->total_read_ns = 0;
	hello_context->total_final_read_ns = 0;

	num_blocks = spdk_bdev_get_num_blocks(hello_context->bdev);
	if (hello_context->repeat_count > 0 &&
	    (hello_context->repeat_count - 1) >
	    (UINT64_MAX - start_offset) / hello_context->stride_blocks) {
		SPDK_ERRLOG("Test offset overflows: offset_blocks=%llu repeat_count=%llu stride_blocks=%llu\n",
			    (unsigned long long)start_offset,
			    (unsigned long long)hello_context->repeat_count,
			    (unsigned long long)hello_context->stride_blocks);
		spdk_bdev_close(hello_context->bdev_desc);
		spdk_app_stop(-1);
		return;
	}

	if (hello_context->prefill_blocks > 0 &&
	    hello_context->prefill_offset > UINT64_MAX - hello_context->prefill_blocks) {
		SPDK_ERRLOG("Prefill offset overflows: offset_blocks=%llu length_blocks=%llu\n",
			    (unsigned long long)hello_context->prefill_offset,
			    (unsigned long long)hello_context->prefill_blocks);
		spdk_bdev_close(hello_context->bdev_desc);
		spdk_app_stop(-1);
		return;
	}

	last_io_end = start_offset + (hello_context->repeat_count - 1) * hello_context->stride_blocks +
		span_length;
	if (hello_context->prefill_blocks > 0) {
		prefill_end = hello_context->prefill_offset + hello_context->prefill_blocks;
		if (last_io_end < prefill_end) {
			last_io_end = prefill_end;
		}
	}
	if (hello_context->final_read_blocks > 0) {
		if (hello_context->final_read_offset > UINT64_MAX - hello_context->final_read_blocks) {
			SPDK_ERRLOG("Final read offset overflows: offset_blocks=%llu length_blocks=%llu\n",
				    (unsigned long long)hello_context->final_read_offset,
				    (unsigned long long)hello_context->final_read_blocks);
			spdk_bdev_close(hello_context->bdev_desc);
			spdk_app_stop(-1);
			return;
		}
		if (last_io_end < hello_context->final_read_offset + hello_context->final_read_blocks) {
			last_io_end = hello_context->final_read_offset + hello_context->final_read_blocks;
		}
	}
	if (last_io_end > num_blocks) {
		SPDK_ERRLOG("Test range exceeds bdev: last_io_end=%llu num_blocks=%llu\n",
			    (unsigned long long)last_io_end,
			    (unsigned long long)num_blocks);
		spdk_bdev_close(hello_context->bdev_desc);
		spdk_app_stop(-1);
		return;
	}


	SPDK_NOTICELOG("Opening io channel\n");
	/* Open I/O channel */
	hello_context->bdev_io_channel = spdk_bdev_get_io_channel(hello_context->bdev_desc);
	if (hello_context->bdev_io_channel == NULL) {
		SPDK_ERRLOG("Could not create bdev I/O channel!!\n");
		spdk_bdev_close(hello_context->bdev_desc);
		spdk_app_stop(-1);
		return;
	}

	/* Allocate memory for the write buffer.
	 * Initialize the write buffer with the string "Hello World!"
	 */
	blk_size = spdk_bdev_get_block_size(hello_context->bdev);
	buf_align = spdk_bdev_get_buf_align(hello_context->bdev);
	hello_context->blk_size = blk_size;
    hello_context->op_buff_len = blk_size * span_length;
    hello_context->final_buff_len = blk_size * hello_context->final_read_blocks;
    hello_context->prefill_buff_len = blk_size * hello_context->prefill_blocks;
    hello_context->buff_len = hello_context->op_buff_len;
	if (hello_context->buff_len < hello_context->final_buff_len) {
		hello_context->buff_len = hello_context->final_buff_len;
	}
	if (hello_context->buff_len < hello_context->prefill_buff_len) {
		hello_context->buff_len = hello_context->prefill_buff_len;
	}
    SPDK_NOTICELOG("Test parameters: offset_blocks=%llu length_blocks=%llu repeat_count=%llu "
		   "stride_blocks=%llu final_read_offset_blocks=%llu "
		   "final_read_length_blocks=%llu prefill_offset_blocks=%llu "
		   "prefill_length_blocks=%llu seed=%u\n",
                   (unsigned long long)start_offset,
                   (unsigned long long)span_length,
                   (unsigned long long)hello_context->repeat_count,
                   (unsigned long long)hello_context->stride_blocks,
                   (unsigned long long)hello_context->final_read_offset,
                   (unsigned long long)hello_context->final_read_blocks,
                   (unsigned long long)hello_context->prefill_offset,
                   (unsigned long long)hello_context->prefill_blocks,
                   g_data_seed);
	hello_context->buff = (char*) spdk_dma_zmalloc(hello_context->buff_len, buf_align, NULL);
	if (!hello_context->buff) {
		SPDK_ERRLOG("Failed to allocate buffer\n");
		spdk_put_io_channel(hello_context->bdev_io_channel);
		spdk_bdev_close(hello_context->bdev_desc);
		spdk_app_stop(-1);
		return;
	}
    hello_context->init_buff = (char*) spdk_dma_zmalloc(hello_context->buff_len, buf_align, NULL);
    if (!hello_context->init_buff) {
        SPDK_ERRLOG("Failed to allocate buffer\n");
        spdk_put_io_channel(hello_context->bdev_io_channel);
        spdk_bdev_close(hello_context->bdev_desc);
        spdk_app_stop(-1);
        return;
    }
	if (hello_context->final_read_blocks > 0) {
		hello_context->final_expected_buff = (char*) calloc(1, hello_context->final_buff_len);
		if (!hello_context->final_expected_buff) {
			SPDK_ERRLOG("Failed to allocate final expected buffer\n");
			spdk_put_io_channel(hello_context->bdev_io_channel);
			spdk_bdev_close(hello_context->bdev_desc);
			spdk_app_stop(-1);
			return;
		}
	}
    srand(g_data_seed);
	if (hello_context->prefill_blocks > 0) {
		hello_context->prefilling = true;
		hello_context->current_offset = hello_context->prefill_offset;
		hello_context->current_io_blocks = hello_context->prefill_blocks;
	} else {
		hello_context->current_io_blocks = span_length;
	}
    prepare_write_buffer(hello_context);
    //show_buffer((unsigned char*)hello_context->init_buff, hello_context->buff_len);
    //snprintf(hello_context->init_buff, blk_size * span_length, "%s", "Hello World!\n");

	//if (spdk_bdev_is_zoned(hello_context->bdev)) {
		//hello_reset_zone(hello_context);
		/* If bdev is zoned, the callback, reset_zone_complete, will call hello_write() */
		//return;
	//}

    hello_write(hello_context);
}

int
main(int argc, char **argv)
{
    //ibv_fork_init();
	struct spdk_app_opts opts = {};
	int rc = 0;
	struct hello_context_t hello_context = {};

	/* Set default values in opts structure. */
	spdk_app_opts_init(&opts, sizeof(opts));
	opts.name = "hello_bdev";

    rc = load_test_parameters_from_env();
    if (rc != 0) {
        exit(rc);
    }

	/*
	 * Parse built-in SPDK command line parameters as well
	 * as our custom one(s).
	 */
    if ((rc = spdk_app_parse_args(argc, argv, &opts, "b:", NULL, hello_bdev_parse_arg,
                                  hello_bdev_usage)) != SPDK_APP_PARSE_ARGS_SUCCESS) {
        exit(rc);
    }
    hello_context.bdev_name = g_bdev_name;

	/*
	 * spdk_app_start() will initialize the SPDK framework, call hello_start(),
	 * and then block until spdk_app_stop() is called (or if an initialization
	 * error occurs, spdk_app_start() will return with rc even without calling
	 * hello_start().
	 */
	rc = spdk_app_start(&opts, hello_start, &hello_context);
	if (rc) {
		SPDK_ERRLOG("ERROR starting application\n");
	}

	/* At this point either spdk_app_stop() was called, or spdk_app_start()
	 * failed because of internal error.
	 */

	/* When the app stops, free up memory that we allocated. */
	spdk_dma_free(hello_context.buff);
    spdk_dma_free(hello_context.init_buff);
	free(hello_context.final_expected_buff);

	/* Gracefully close out all of the SPDK subsystems. */
	spdk_app_fini();
	return rc;
}

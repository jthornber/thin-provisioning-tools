#include "block-cache/block_cache.h"

#include "block-cache/list.h"

#include <assert.h>
#include <libaio.h>
#include <errno.h>
#include <pthread.h>
#include <stdarg.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>

// FIXME: get from linux headers
#define SECTOR_SHIFT 9
#define PAGE_SIZE 4096

#define MIN_BLOCKS 16
#define WRITEBACK_LOW_THRESHOLD_PERCENT 33
#define WRITEBACK_HIGH_THRESHOLD_PERCENT 66

/*----------------------------------------------------------------
 * Structures
 *--------------------------------------------------------------*/
struct block_cache;

enum block_flags {
	IO_PENDING = (1 << 0),
	DIRTY = (1 << 1)
};

struct block {
	struct list_head list;
	struct list_head hash_list;

	struct block_cache *bc;
	unsigned ref_count;

	int error;
	unsigned flags;

	struct iocb control_block;

	struct bc_block b;
};

struct block_cache {
	int fd;
	sector_t block_size;
	uint64_t nr_data_blocks;
	uint64_t nr_cache_blocks;

	void *blocks_memory;
	void *blocks_data;

	io_context_t aio_context;
	struct io_event *events;

	/*
	 * Blocks on the free list are not initialised, apart from the
	 * b.data field.
	 */
	struct list_head free;
	struct list_head errored;
	struct list_head dirty;
	struct list_head clean;

	unsigned nr_io_pending;
	struct list_head io_pending;

	unsigned nr_dirty;

	/*
	 * Hash table fields.
	 */
	unsigned nr_buckets;
	unsigned mask;
	struct list_head buckets[0];
};

/*----------------------------------------------------------------
 * Logging
 *--------------------------------------------------------------*/
static void info(struct block_cache *bc, const char *format, ...)
	__attribute__ ((format (printf, 2, 3)));

static void info(struct block_cache *bc, const char *format, ...)
{
	va_list ap;

	va_start(ap, format);
	vfprintf(stderr, format, ap);
	va_end(ap);
}

/*----------------------------------------------------------------
 * Allocation
 *--------------------------------------------------------------*/
static void *alloc_aligned(size_t len, size_t alignment)
{
	void *result = NULL;
	int r = posix_memalign(&result, alignment, len);
	if (r)
		return NULL;

	return result;
}

static int init_free_list(struct block_cache *bc, unsigned count)
{
	size_t len;
	struct block *blocks;
	size_t block_size = bc->block_size << SECTOR_SHIFT;
	void *data;
	unsigned i;

	/* Allocate the block structures */
	len = sizeof(struct block) * count;
	blocks = static_cast<block *>(malloc(len));
	if (!blocks)
		return -ENOMEM;

	bc->blocks_memory = blocks;

	/* Allocate the data for each block.  We page align the data. */
	data = alloc_aligned(count * block_size, PAGE_SIZE);
	if (!data) {
		free(blocks);
		return -ENOMEM;
	}

	bc->blocks_data = data;

	for (i = 0; i < count; i++) {
		struct block *b = blocks + i;
		INIT_LIST_HEAD(&b->list);
		b->b.data = data + block_size * i;

		list_add(&b->list, &bc->free);
	}

	return 0;
}

static struct block *__alloc_block(struct block_cache *bc)
{
	struct block *b;

	if (list_empty(&bc->free))
		return NULL;

	b = list_first_entry(&bc->free, struct block, list);
	list_del(&b->list);

	return b;
}

/*----------------------------------------------------------------
 * Flags handling
 *--------------------------------------------------------------*/
static unsigned test_flags(struct block *b, unsigned flags)
{
	return b->flags & flags;
}

static void clear_flags(struct block *b, unsigned flags)
{
	b->flags &= ~flags;
}

static void set_flags(struct block *b, unsigned flags)
{
	b->flags |= flags;
}

/*----------------------------------------------------------------
 * Low level IO handling
 *
 * We cannot have two concurrent writes on the same block.
 * eg, background writeback, put with dirty, flush?
 *
 * To avoid this we introduce some restrictions:
 *
 * i)  A held block can never be written back.
 * ii) You cannot get a block until writeback has completed.
 *
 *--------------------------------------------------------------*/

/*
 * This can be called from the context of the aio thread.  So we have a
 * separate 'top half' complete function that we know is only called by the
 * main cache thread.
 */
static void complete_io(struct block *b, int result)
{
	b->error = result;
	clear_flags(b, IO_PENDING);
	b->bc->nr_io_pending--;

	if (b->error)
		list_move_tail(&b->list, &b->bc->errored);
	else {
		if (test_flags(b, DIRTY)) {
			clear_flags(b, DIRTY);
			b->bc->nr_dirty--;
		}

		list_move_tail(&b->list, &b->bc->clean);
	}
}

/*
 * |b->list| should be valid (either pointing to itself, on one of the other
 * lists.
 */
static int issue_low_level(struct block *b, enum io_iocb_cmd opcode, const char *desc)
{
	int r;
	struct block_cache *bc = b->bc;
	struct iocb *control_blocks[1];

	assert(!test_flags(b, IO_PENDING));
	set_flags(b, IO_PENDING);
	bc->nr_io_pending++;
	list_move_tail(&b->list, &bc->io_pending);

	b->control_block.aio_lio_opcode = opcode;
	control_blocks[0] = &b->control_block;
	r = io_submit(bc->aio_context, 1, control_blocks);
	if (r != 1) {
		if (r < 0) {
			perror("io_submit error");
			info(bc, "io_submit failed with %s op: %d\n", desc, r);
		} else
			info(bc, "could not submit IOs, with %s op\n", desc);

		complete_io(b, EIO);
		return -EIO;
	}

	return 0;
}

static int issue_read(struct block *b)
{
	return issue_low_level(b, IO_CMD_PREAD, "read");
}

static int issue_write(struct block *b)
{
	return issue_low_level(b, IO_CMD_PWRITE, "write");
}

static void wait_io(struct block_cache *bc)
{
	int r;
	unsigned i;

	// FIXME: use a timeout to prevent hanging
	r = io_getevents(bc->aio_context, 1, bc->nr_cache_blocks, bc->events, NULL);
	if (r < 0) {
		info(bc, "io_getevents failed %d\n", r);
		exit(1);	/* FIXME: handle more gracefully */
	}

	for (i = 0; i < static_cast<unsigned>(r); i++) {
		struct io_event *e = bc->events + i;
		struct block *b = container_of(e->obj, struct block, control_block);

		if (e->res == bc->block_size << SECTOR_SHIFT)
			complete_io(b, 0);
		else if (e->res < 0)
			complete_io(b, e->res);
		else {
			info(bc, "incomplete io, unexpected\n");
		}
	}
}

/*----------------------------------------------------------------
 * Clean/dirty list management
 *--------------------------------------------------------------*/

/*
 * We're using lru lists atm, but I think it would be worth
 * experimenting with a multiqueue approach.
 */
static struct list_head *__categorise(struct block *b)
{
	if (b->error)
		return &b->bc->errored;

	return (b->flags & DIRTY) ? &b->bc->dirty : &b->bc->clean;
}

static void hit(struct block *b)
{
	list_move_tail(&b->list, __categorise(b));
}

/*----------------------------------------------------------------
 * High level IO handling
 *--------------------------------------------------------------*/
static void wait_all(struct block_cache *bc)
{
	while (!list_empty(&bc->io_pending))
		wait_io(bc);
}

static void wait_specific(struct block *b)
{
	while (test_flags(b, IO_PENDING))
		wait_io(b->bc);
}

static unsigned writeback(struct block_cache *bc, unsigned count)
{
	int r;
	struct block *b, *tmp;
	unsigned actual = 0;

	list_for_each_entry_safe (b, tmp, &bc->dirty, list) {
		if (actual == count)
			break;

		if (b->ref_count)
			continue;

		r = issue_write(b);
		if (!r)
			actual++;
	}

	info(bc, "writeback: requested %u, actual %u\n", count, actual);
	return actual;
}

/*----------------------------------------------------------------
 * Hash table
 *---------------------------------------------------------------*/

/*
 * |nr_buckets| must be a power of two.
 */
static void hash_init(struct block_cache *bc, unsigned nr_buckets)
{
	unsigned i;

	bc->nr_buckets = nr_buckets;
	bc->mask = nr_buckets - 1;

	for (i = 0; i < nr_buckets; i++)
		INIT_LIST_HEAD(bc->buckets + i);
}

static unsigned hash(struct block_cache *bc, uint64_t index)
{
	const unsigned BIG_PRIME = 4294967291UL;
	return (((unsigned) index) * BIG_PRIME) & bc->mask;
}

static struct block *hash_lookup(struct block_cache *bc, block_index index)
{
	struct block *b;
	unsigned bucket = hash(bc, index);

	list_for_each_entry (b, bc->buckets + bucket, hash_list) {
		if (b->b.index == index)
			return b;
	}

	return NULL;
}

static void hash_insert(struct block *b)
{
	unsigned bucket = hash(b->bc, b->b.index);

	list_move_tail(&b->hash_list, b->bc->buckets + bucket);
}

static void hash_remove(struct block *b)
{
	list_del_init(&b->hash_list);
}

/*----------------------------------------------------------------
 * High level allocation
 *--------------------------------------------------------------*/
static void setup_control_block(struct block *b)
{
	struct iocb *cb = &b->control_block;
	size_t block_size_bytes = b->bc->block_size << SECTOR_SHIFT;

	memset(cb, 0, sizeof(*cb));
	cb->aio_fildes = b->bc->fd;

	cb->u.c.buf = b->b.data;
	cb->u.c.offset = block_size_bytes * b->b.index;
	cb->u.c.nbytes = block_size_bytes;
}

static struct block *new_block(struct block_cache *bc,
			       block_index index)
{
	struct block *b;

	b = __alloc_block(bc);
	if (!b) {
		if (list_empty(&bc->clean)) {
			if (list_empty(&bc->io_pending))
				writeback(bc, 9000);
			wait_io(bc);
		}

		if (!list_empty(&bc->clean)) {
			b = list_first_entry(&bc->clean, struct block, list);
			hash_remove(b);
			list_del(&b->list);
		}
	}

	if (b) {
		INIT_LIST_HEAD(&b->list);
		INIT_LIST_HEAD(&b->hash_list);
		b->bc = bc;
		b->ref_count = 0;

		b->error = 0;
		clear_flags(b, IO_PENDING | DIRTY);

		b->b.index = index;
		setup_control_block(b);

		hash_insert(b);
	}

	return b;
}

/*----------------------------------------------------------------
 * Block reference counting
 *--------------------------------------------------------------*/
static void get_block(struct block *b)
{
	b->ref_count++;
}

static void put_block(struct block *b)
{
	assert(b->ref_count);
	b->ref_count--;
}

static void mark_dirty(struct block *b)
{
	struct block_cache *bc = b->bc;

	if (!test_flags(b, DIRTY)) {
		set_flags(b, DIRTY);
		list_move_tail(&b->list, &b->bc->dirty);
		bc->nr_dirty++;
	}
}

/*----------------------------------------------------------------
 * Public interface
 *--------------------------------------------------------------*/
unsigned calc_nr_cache_blocks(size_t mem, sector_t block_size)
{
	size_t space_per_block = (block_size << SECTOR_SHIFT) + sizeof(struct block);
	unsigned r = mem / space_per_block;

	return (r < MIN_BLOCKS) ? MIN_BLOCKS : r;
}

unsigned calc_nr_buckets(unsigned nr_blocks)
{
	unsigned r = 8;
	unsigned n = nr_blocks / 4;

	if (n < 8)
		n = 8;

	while (r < n)
		r <<= 1;

	return r;
}

void
block_cache_destroy(struct block_cache *bc)
{
	wait_all(bc);

	if (bc->aio_context)
		io_destroy(bc->aio_context);

	if (bc->events)
		free(bc->events);

	if (bc->blocks_memory)
		free(bc->blocks_memory);

	if (bc->blocks_data)
		free(bc->blocks_data);

	free(bc);
}

struct block_cache *
block_cache_create(int fd, sector_t block_size, uint64_t on_disk_blocks, size_t mem)
{
	int r;
	struct block_cache *bc;
	unsigned nr_cache_blocks = calc_nr_cache_blocks(mem, block_size);
	unsigned nr_buckets = calc_nr_buckets(nr_cache_blocks);

	bc = static_cast<block_cache *>(malloc(sizeof(*bc) + sizeof(*bc->buckets) * nr_buckets));
	if (bc) {
		memset(bc, 0, sizeof(*bc));

		bc->fd = fd;
		bc->block_size = block_size;
		bc->nr_data_blocks = on_disk_blocks;
		bc->nr_cache_blocks = nr_cache_blocks;

		bc->events = static_cast<io_event *>(malloc(sizeof(*bc->events) * nr_cache_blocks));
		if (!bc->events) {
			info(bc, "couldn't allocate events array\n");
			goto bad;
		}

		bc->aio_context = 0; /* needed or io_setup will fail */
		r = io_setup(nr_cache_blocks, &bc->aio_context);
		if (r < 0) {
			info(bc, "io_setup failed: %d\n", r);
			goto bad;
		}

		hash_init(bc, nr_buckets);
		INIT_LIST_HEAD(&bc->free);
		INIT_LIST_HEAD(&bc->errored);
		INIT_LIST_HEAD(&bc->dirty);
		INIT_LIST_HEAD(&bc->clean);
		INIT_LIST_HEAD(&bc->io_pending);

		r = init_free_list(bc, nr_cache_blocks);
		if (r) {
			info(bc, "couldn't allocate blocks: %d\n", r);
			goto bad;
		}
	}

	return bc;

bad:
	block_cache_destroy(bc);
	return NULL;
}

uint64_t block_cache_get_nr_blocks(struct block_cache *bc)
{
	return bc->nr_data_blocks;
}

static void zero_block(struct block *b)
{
	memset(b->b.data, 0, b->bc->block_size << SECTOR_SHIFT);
	mark_dirty(b);
}

static struct block *lookup_or_read_block(struct block_cache *bc, block_index index, unsigned flags)
{
	struct block *b = hash_lookup(bc, index);

	if (b) {
		if (test_flags(b, IO_PENDING))
			wait_specific(b);

		if (flags & GF_ZERO)
			zero_block(b);

	} else {
		if (flags & GF_CAN_BLOCK) {
			b = new_block(bc, index);
			if (b) {
				if (flags & GF_ZERO)
					zero_block(b);
				else {
					issue_read(b);
					wait_specific(b);
				}
			}
		}
	}

	return (!b || b->error) ? NULL : b;
}

struct bc_block *
block_cache_get(struct block_cache *bc, block_index index, unsigned flags)
{
	struct block *b = lookup_or_read_block(bc, index, flags);

	if (b) {
		hit(b);
		get_block(b);

		return &b->b;
	}

	return NULL;
}

void
block_cache_put(struct bc_block *bcb, unsigned flags)
{
	unsigned nr_available;
	struct block *b = container_of(bcb, struct block, b);
	struct block_cache *bc = b->bc;

	put_block(b);

	if (flags & PF_DIRTY) {
		mark_dirty(b);

		nr_available = bc->nr_cache_blocks - (bc->nr_dirty - bc->nr_io_pending);
		if (nr_available < (WRITEBACK_LOW_THRESHOLD_PERCENT * bc->nr_cache_blocks / 100))
			writeback(bc, (WRITEBACK_HIGH_THRESHOLD_PERCENT * bc->nr_cache_blocks / 100) - nr_available);
	}
}

int
block_cache_flush(struct block_cache *bc)
{
	struct block *b;

	list_for_each_entry (b, &bc->dirty, list) {
		if (b->ref_count) {
			info(bc, "attempt to lock an already locked block\n");
			return -EAGAIN;
		}

		issue_write(b);
	}

	wait_all(bc);

	return list_empty(&bc->errored) ? 0 : -EIO;
}

void
block_cache_prefetch(struct block_cache *bc, block_index index)
{
	struct block *b = hash_lookup(bc, index);

	if (!b) {
		b = new_block(bc, index);
		if (b)
			issue_read(b);
	}
}

/*----------------------------------------------------------------*/


#define _GNU_SOURCE

#include <assert.h>
#include <errno.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <stdbool.h>
#include <stdlib.h>
#include <stdio.h>
#include <stdint.h>
#include <libaio.h>
#include <unistd.h>
#include <linux/fs.h>
#include <sys/ioctl.h>

#include "list.h"
#include "bcache.h"

//----------------------------------------------------------------

static void warn(const char *msg)
{
	fprintf(stderr, "%s\n", msg);
}

// FIXME: raise a condition somehow?
static void raise(const char *msg)
{
	warn(msg);
	exit(1);
}

/*
 * Assumes the list is not empty.
 */
static inline struct list_head *list_pop(struct list_head *head)
{
	struct list_head *l;

	l = head->next;
	list_del(l);
	return l;
}

//----------------------------------------------------------------

struct control_block {
	struct list_head list;
	void *context;
	struct iocb cb;
};

struct cb_set {
	struct list_head free;
	struct list_head allocated;
	struct control_block *vec;
} control_block_set;

static struct cb_set *cb_set_create(unsigned nr)
{
	int i;
	struct cb_set *cbs = malloc(sizeof(*cbs));

	if (!cbs)
		return NULL;

	cbs->vec = malloc(nr * sizeof(*cbs->vec));
	if (!cbs->vec) {
		free(cbs);
		return NULL;
	}

	init_list_head(&cbs->free);
	init_list_head(&cbs->allocated);

	for (i = 0; i < nr; i++)
		list_add(&cbs->vec[i].list, &cbs->free);

	return cbs;
}

static void cb_set_destroy(struct cb_set *cbs)
{
	if (!list_empty(&cbs->allocated))
		raise("async io still in flight");

	free(cbs->vec);
	free(cbs);
}

static struct control_block *cb_alloc(struct cb_set *cbs, void *context)
{
	struct control_block *cb;

	if (list_empty(&cbs->free))
		return NULL;

	cb = container_of(list_pop(&cbs->free), struct control_block, list);
	cb->context = context;
	list_add(&cb->list, &cbs->allocated);

	return cb;
}

static void cb_free(struct cb_set *cbs, struct control_block *cb)
{
	list_del(&cb->list);
	list_add(&cb->list, &cbs->free);
}

static struct control_block *iocb_to_cb(struct iocb *icb)
{
	return container_of(icb, struct control_block, cb);
}

//----------------------------------------------------------------

// FIXME: get from linux headers
#define SECTOR_SHIFT 9
#define PAGE_SIZE 4096

enum dir {
	DIR_READ,
	DIR_WRITE
};

struct io_engine {
	io_context_t aio_context;
	struct cb_set *cbs;
};

static struct io_engine *engine_create(unsigned max_io)
{
	int r;
	struct io_engine *e = malloc(sizeof(*e));

	if (!e)
		return NULL;

	e->aio_context = 0;
	r = io_setup(max_io, &e->aio_context);
	if (r < 0) {
		warn("io_setup failed");
		return NULL;
	}

	e->cbs = cb_set_create(max_io);
	if (!e->cbs) {
		warn("couldn't create control block set");
		free(e);
		return NULL;
	}

	return e;
}

static void engine_destroy(struct io_engine *e)
{
	cb_set_destroy(e->cbs);
	io_destroy(e->aio_context);
	free(e);
}

static int engine_issue(struct io_engine *e, int fd, enum dir d,
			sector_t sb, sector_t se, void *data, void *context)
{
	int r;
	struct iocb *cb_array[1];
	struct control_block *cb;

	if (((uint64_t) data) & (PAGE_SIZE - 1))
		return -1;

	cb = cb_alloc(e->cbs, context);
	if (!cb)
		return false;

	memset(&cb->cb, 0, sizeof(cb->cb));

	cb->cb.aio_fildes = (int) fd;
	cb->cb.u.c.buf = data;
	cb->cb.u.c.offset = sb << SECTOR_SHIFT;
	cb->cb.u.c.nbytes = (se - sb) << SECTOR_SHIFT;
	cb->cb.aio_lio_opcode = (d == DIR_READ) ? IO_CMD_PREAD : IO_CMD_PWRITE;

	cb_array[0] = &cb->cb;
	r = io_submit(e->aio_context, 1, cb_array);
	if (r < 0)
		cb_free(e->cbs, cb);

	return r;
}

#define MAX_IO 64
typedef void complete_fn(void *context, int io_error);

static int engine_wait(struct io_engine *e, struct timespec *ts, complete_fn fn)
{
	int i, r;
	struct io_event event[MAX_IO];
	struct control_block *cb;

	memset(&event, 0, sizeof(event));
	r = io_getevents(e->aio_context, 1, MAX_IO, event, ts);
	if (r < 0) {
		warn("io_getevents failed");
		return r;
	}

	if (r == 0)
		return 0;

	for (i = 0; i < r; i++) {
		struct io_event *ev = event + i;

		cb = iocb_to_cb((struct iocb *) ev->obj);

		if (ev->res == cb->cb.u.c.nbytes)
			fn((void *) cb->context, 0);

		else if ((int) ev->res < 0)
			fn(cb->context, (int) ev->res);

		else {
			warn("short io");
			fn(cb->context, -ENODATA);
		}

		cb_free(e->cbs, cb);
	}

	return -ENODATA;
}

//----------------------------------------------------------------
#if 0
struct timespec micro_to_ts(unsigned micro)
{
	struct timespec ts;
	ts.tv_sec = micro / 1000000u;
	ts.tv_nsec = (micro % 1000000) * 1000;
	return ts;
}

static unsigned ts_to_micro(struct timespec const *ts)
{
	unsigned micro = ts->tv_sec * 1000000;
	micro += ts->tv_nsec / 1000;
	return micro;
}
#endif
//----------------------------------------------------------------

#define MIN_BLOCKS 16
#define WRITEBACK_LOW_THRESHOLD_PERCENT 33
#define WRITEBACK_HIGH_THRESHOLD_PERCENT 66

//----------------------------------------------------------------

static void *alloc_aligned(size_t len, size_t alignment)
{
	void *result = NULL;
	int r = posix_memalign(&result, alignment, len);
	if (r)
		return NULL;

	return result;
}

//----------------------------------------------------------------

static bool test_flags(struct block *b, unsigned bits)
{
	return (b->flags & bits) != 0;
}

static void set_flags(struct block *b, unsigned bits)
{
	b->flags |= bits;
}

static void clear_flags(struct block *b, unsigned bits)
{
	b->flags &= ~bits;
}

//----------------------------------------------------------------

enum block_flags {
	BF_IO_PENDING = (1 << 0),
	BF_DIRTY = (1 << 1),
};

struct bcache {
	int fd;
	sector_t block_sectors;
	uint64_t nr_data_blocks;
	uint64_t nr_cache_blocks;

	struct io_engine *engine;

	void *raw_data;
	struct block *raw_blocks;

	/*
	 * Lists that categorise the blocks.
	 */
	unsigned nr_locked;
	unsigned nr_dirty;
	unsigned nr_io_pending;

	struct list_head free;
	struct list_head errored;
	struct list_head dirty;
	struct list_head clean;
	struct list_head io_pending;

	/*
	 * Hash table.
	 */
	unsigned nr_buckets;
	unsigned hash_mask;
	struct list_head *buckets;

	/*
	 * Statistics
	 */
	unsigned read_hits;
	unsigned read_misses;
	unsigned write_zeroes;
	unsigned write_hits;
	unsigned write_misses;
	unsigned prefetches;
};

//----------------------------------------------------------------

/*  2^63 + 2^61 - 2^57 + 2^54 - 2^51 - 2^18 + 1 */
#define GOLDEN_RATIO_PRIME_64 0x9e37fffffffc0001UL

static unsigned hash(struct bcache *cache, uint64_t index)
{
	uint64_t h = index;
	h *= GOLDEN_RATIO_PRIME_64;
	return h & cache->hash_mask;
}

static struct block *hash_lookup(struct bcache *cache, uint64_t index)
{
	struct block *b;
	unsigned h = hash(cache, index);

	list_for_each_entry (b, cache->buckets + h, hash)
		if (b->index == index)
			return b;

	return NULL;
}

static void hash_insert(struct block *b)
{
	unsigned h = hash(b->cache, b->index);
	list_add(&b->hash, b->cache->buckets + h);
}

static void hash_remove(struct block *b)
{
	list_del(&b->hash);
}

/*
 * Must return a power of 2.
 */
static unsigned calc_nr_buckets(unsigned nr_blocks)
{
	unsigned r = 8;
	unsigned n = nr_blocks / 4;

	if (n < 8)
		n = 8;

	while (r < n)
		r <<= 1;

	return r;
}

static int hash_table_init(struct bcache *cache, unsigned nr_entries)
{
	unsigned i;

	cache->nr_buckets = calc_nr_buckets(nr_entries);
	cache->hash_mask = cache->nr_buckets - 1;
	cache->buckets = malloc(cache->nr_buckets * sizeof(*cache->buckets));
	if (!cache->buckets)
		return -ENOMEM;

	for (i = 0; i < cache->nr_buckets; i++)
		init_list_head(cache->buckets + i);

	return 0;
}

static void hash_table_exit(struct bcache *cache)
{
	free(cache->buckets);
}

//----------------------------------------------------------------

static int init_free_list(struct bcache *cache, unsigned count)
{
	unsigned i;
	size_t block_size = cache->block_sectors << SECTOR_SHIFT;
	unsigned char *data =
		(unsigned char *) alloc_aligned(count * block_size, PAGE_SIZE);

	/* Allocate the data for each block.  We page align the data. */
	if (!data)
		return -ENOMEM;

	cache->raw_data = data;
	cache->raw_blocks = malloc(count * sizeof(*cache->raw_blocks));

	if (!cache->raw_blocks)
		free(cache->raw_data);

	for (i = 0; i < count; i++) {
		struct block *b = cache->raw_blocks + i;
		b->cache = cache;
		b->data = data + (block_size * i);
		list_add_tail(&b->list, &cache->free);
	}

	return 0;
}

static void exit_free_list(struct bcache *cache)
{
	free(cache->raw_data);
	free(cache->raw_blocks);
}

static struct block *alloc_block(struct bcache *cache)
{
	struct block *b = container_of(list_pop(&cache->free), struct block, list);
	return b;
}

/*----------------------------------------------------------------
 * Clean/dirty list management.
 * Always use these methods to ensure nr_dirty_ is correct.
 *--------------------------------------------------------------*/

static void unlink_block(struct block *b)
{
	if (test_flags(b, BF_DIRTY))
		b->cache->nr_dirty--;

	list_del(&b->list);
}

static void link_block(struct block *b)
{
	struct bcache *cache = b->cache;

	if (test_flags(b, BF_DIRTY)) {
		list_add_tail(&b->list, &cache->dirty);
		cache->nr_dirty++;
	} else
		list_add_tail(&b->list, &cache->clean);
}

static void relink(struct block *b)
{
	unlink_block(b);
	link_block(b);
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
 * |b->list| should be valid (either pointing to itself, on one of the other
 * lists.
 */
static int issue_low_level(struct block *b, enum dir d)
{
	struct bcache *cache = b->cache;
	sector_t sb = b->index * cache->block_sectors;
	sector_t se = sb + cache->block_sectors;
	set_flags(b, BF_IO_PENDING);

	return engine_issue(cache->engine, cache->fd, d, sb, se, b->data, b);
}

static void issue_read(struct block *b)
{
	assert(!test_flags(b, BF_IO_PENDING));
	issue_low_level(b, DIR_READ);
}

static void issue_write(struct block *b)
{
	assert(!test_flags(b, BF_IO_PENDING));
	//b.v_->prepare(b.data_, b.index_);
	issue_low_level(b, DIR_WRITE);
}

static void complete_io(void *context, int err)
{
	struct block *b = context;
	struct bcache *cache = b->cache;

	b->error = err;
	clear_flags(b, BF_IO_PENDING);
	cache->nr_io_pending--;

	/*
	 * b is on the io_pending list, so we don't want to use unlink_block.
	 * Which would incorrectly adjust nr_dirty.
	 */
	list_del(&b->list);

	if (b->error)
		list_add_tail(&b->list, &cache->errored);

	else {
		clear_flags(b, BF_DIRTY);
		link_block(b);
	}
}

static int wait_io(struct bcache *cache)
{
	return engine_wait(cache->engine, NULL, complete_io);
}

/*----------------------------------------------------------------
 * High level IO handling
 *--------------------------------------------------------------*/

static void wait_all(struct bcache *cache)
{
	while (!list_empty(&cache->io_pending))
		wait_io(cache);
}

static void wait_specific(struct block *b)
{
	while (test_flags(b, BF_IO_PENDING))
		wait_io(b->cache);
}

static unsigned writeback(struct bcache *cache, unsigned count)
{
	unsigned actual = 0;
	struct block *b, *tmp;

	list_for_each_entry_safe (b, tmp, &cache->dirty, list) {
		if (actual == count)
			break;

		// We can't writeback anything that's still in use.
		if (!b->ref_count) {
			issue_write(b);
			actual++;
		}
	}

	return actual;
}

/*----------------------------------------------------------------
 * High level allocation
 *--------------------------------------------------------------*/

static struct block *find_unused_clean_block(struct bcache *cache)
{
	struct block *b;

	list_for_each_entry (b, &cache->clean, list) {
		if (!b->ref_count) {
			unlink_block(b);
			hash_remove(b);
			return b;
		}
	}

	return NULL;
}

static struct block *new_block(struct bcache *cache, block_address index)
{
	struct block *b;

	b = alloc_block(cache);
	while (!b && cache->nr_locked < cache->nr_cache_blocks) {
		b = find_unused_clean_block(cache);
		if (!b) {
			if (list_empty(&cache->io_pending))
				writeback(cache, 16);
			wait_io(cache);
		}
	}

	if (b) {
		init_list_head(&b->list);
		init_list_head(&b->hash);
		b->flags = 0;
		b->index = index;
		b->ref_count = 0;
		b->error = 0;

		hash_insert(b);
	}

	return b;
}

/*----------------------------------------------------------------
 * Block reference counting
 *--------------------------------------------------------------*/
struct bcache *bcache_create(int fd, sector_t block_sectors, uint64_t on_disk_blocks,
			     unsigned nr_cache_blocks)
{
	int r;
	struct bcache *cache;

	cache = malloc(sizeof(*cache));
	if (!cache)
		return NULL;

	cache->fd = fd;
	cache->block_sectors = block_sectors;
	cache->nr_data_blocks = on_disk_blocks;
	cache->nr_cache_blocks = nr_cache_blocks;

	cache->engine = engine_create(nr_cache_blocks < 1024u ? nr_cache_blocks : 1024u);
	if (!cache->engine) {
		free(cache);
		return NULL;
	}

	cache->nr_locked = 0;
	cache->nr_dirty = 0;
	cache->nr_io_pending = 0;

	init_list_head(&cache->free);
	init_list_head(&cache->errored);
	init_list_head(&cache->dirty);
	init_list_head(&cache->clean);
	init_list_head(&cache->io_pending);

	if (hash_table_init(cache, nr_cache_blocks)) {
		engine_destroy(cache->engine);
		free(cache);
	}

	cache->read_hits = 0;
	cache->read_misses = 0;
	cache->write_zeroes = 0;
	cache->write_hits = 0;
	cache->write_misses = 0;
	cache->prefetches = 0;

	r = init_free_list(cache, nr_cache_blocks);
	if (r) {
		engine_destroy(cache->engine);
		hash_table_exit(cache);
		free(cache);
	}

	return cache;
}

#define MD_BLOCK_SIZE 4096ull

struct bcache *bcache_simple(const char *path, unsigned nr_cache_blocks)
{
	int r;
	struct stat info;
	struct bcache *cache;
	int fd = open(path, O_DIRECT | O_EXCL | O_RDONLY);
	uint64_t s;

	if (fd < 0) {
		raise("couldn't open cache file");
		return NULL;
	}

	r = fstat(fd, &info);
	if (r < 0) {
		raise("couldn't stat cache file");
		return NULL;
	}

	s = info.st_size;
	cache = bcache_create(fd, MD_BLOCK_SIZE >> SECTOR_SHIFT,
			      s / MD_BLOCK_SIZE, nr_cache_blocks);
	if (!cache)
		close(fd);

	return cache;
}

void bcache_destroy(struct bcache *cache)
{
	if (cache->nr_locked)
		warn("some blocks are still locked\n");

	flush_cache(cache);
	wait_all(cache);
	exit_free_list(cache);
	hash_table_exit(cache);
	engine_destroy(cache->engine);
	close(cache->fd);
	free(cache);
}

// FIXME: we have to return an error code that can be turned into a Scheme
// condition.
static void check_index(struct bcache *cache, block_address index)
{
	if (index >= cache->nr_data_blocks)
		raise("block out of bounds");
}

uint64_t get_nr_blocks(struct bcache *cache)
{
	return cache->nr_data_blocks;
}

uint64_t get_nr_locked(struct bcache *cache)
{
	return cache->nr_locked;
}

static void zero_block(struct block *b)
{
	b->cache->write_zeroes++;
	memset(b->data, 0, b->cache->block_sectors << SECTOR_SHIFT);
	set_flags(b, BF_DIRTY);
}

static void hit(struct block *b, unsigned flags)
{
	struct bcache *cache = b->cache;

	if (flags & (GF_ZERO | GF_DIRTY))
		cache->write_hits++;
	else
		cache->read_hits++;

	relink(b);
}

static void miss(struct bcache *cache, unsigned flags)
{
	if (flags & (GF_ZERO | GF_DIRTY))
		cache->write_misses++;
	else
		cache->read_misses++;
}

static struct block *lookup_or_read_block(struct bcache *cache,
				  	  block_address index, unsigned flags)
{
	struct block *b = hash_lookup(cache, index);

	if (b) {
		// FIXME: this is insufficient.  We need to also catch a read
		// lock of a write locked block.  Ref count needs to distinguish.
		if (b->ref_count && (flags & (GF_DIRTY | GF_ZERO)))
			raise("concurrent write lock attempt");

		if (test_flags(b, BF_IO_PENDING)) {
			miss(cache, flags);
			wait_specific(b);

		} else
			hit(b, flags);

		unlink_block(b);

		if (flags & GF_ZERO)
			zero_block(b);

	} else {
		miss(cache, flags);

		b = new_block(cache, index);
		if (b) {
			if (flags & GF_ZERO)
				zero_block(b);

			else {
				issue_read(b);
				wait_specific(b);

				// we know the block is clean and unerrored.
				unlink_block(b);
			}
		}
	}

	if (b && !b->error) {
		if (flags & (GF_DIRTY | GF_ZERO))
			set_flags(b, BF_DIRTY);

		link_block(b);
		return b;
	}

	return NULL;
}

struct block *get_block(struct bcache *cache, block_address index, unsigned flags)
{
	check_index(cache, index);

	struct block *b = lookup_or_read_block(cache, index, flags);
	if (b) {
		if (!b->ref_count)
			cache->nr_locked++;
		b->ref_count++;

		return b;
	}

	raise("couldn't get block");
	return NULL;
}

static void preemptive_writeback(struct bcache *cache)
{
	// FIXME: this ignores those blocks that are in the error state.  Track
	// nr_clean instead?
	unsigned nr_available = cache->nr_cache_blocks - (cache->nr_dirty - cache->nr_io_pending);
	if (nr_available < (WRITEBACK_LOW_THRESHOLD_PERCENT * cache->nr_cache_blocks / 100))
		writeback(cache, (WRITEBACK_HIGH_THRESHOLD_PERCENT * cache->nr_cache_blocks / 100) - nr_available);

}

void release_block(struct block *b)
{
	assert(b->ref_count);

	b->ref_count--;
	if (!b->ref_count)
		b->cache->nr_locked--;

	if (test_flags(b, BF_DIRTY))
		preemptive_writeback(b->cache);
}

int flush_cache(struct bcache *cache)
{
	while (!list_empty(&cache->dirty)) {
		struct block *b = container_of(list_pop(&cache->dirty), struct block, list);
		if (b->ref_count || test_flags(b, BF_IO_PENDING))
			// The superblock may well be still locked.
			continue;

		issue_write(b);
	}

	wait_all(cache);

	return list_empty(&cache->errored) ? 0 : -EIO;
}

void prefetch_block(struct bcache *cache, block_address index)
{
	check_index(cache, index);
	struct block *b = hash_lookup(cache, index);

	if (!b) {
		cache->prefetches++;

		b = new_block(cache, index);
		if (b)
			issue_read(b);
	}
}

//----------------------------------------------------------------

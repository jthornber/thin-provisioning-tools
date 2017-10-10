#ifndef BCACHE_H
#define BCACHE_H

#include <stdint.h>

/*----------------------------------------------------------------*/

typedef uint64_t block_address;
typedef uint64_t sector_t;

struct block;
struct bcache;

// FIXME: allow the cache to be opened read only.
struct bcache *bcache_create(int fd, sector_t block_size,
			     uint64_t on_disk_blocks,
			     unsigned nr_cache_blocks);

/*
 * A simpler way of creating a bcache that assumes 4k block size, and stats to
 * get the file size.
 */
struct bcache *bcache_simple(const char *path, unsigned nr_cache_blocks);

void bcache_destroy(struct bcache *cache);
uint64_t get_nr_blocks(struct bcache *cache);
uint64_t get_nr_locked(struct bcache *cache);

int flush_cache(struct bcache *cache);

struct bcache;

struct block {
	/* clients may only access these two fields */
	void *data;
	uint64_t index;

	struct bcache *cache;
	struct list_head list;
	struct list_head hash;

	unsigned flags;
	unsigned ref_count;
	int error;
};

enum get_flags {
	/*
	 * The block will be zeroed before get_block returns it.  This
	 * potentially avoids a read if the block is not already in the cache.
	 * GF_DIRTY is implicit.
	 */
	GF_ZERO = (1 << 0),

	/*
	 * Indicates the caller is intending to change the data in the block, a
	 * writeback will occur after the block is released.
	 */
	GF_DIRTY = (1 << 1),
};

struct block *get_block(struct bcache *cache, block_address index, unsigned flags);
void prefetch_block(struct bcache *cache, block_address index);

void release_block(struct block *b);

/*----------------------------------------------------------------*/

#endif

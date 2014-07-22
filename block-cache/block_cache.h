#ifndef BLOCK_CACHE_H
#define BLOCK_CACHE_H

#include <stdint.h>
#include <stdlib.h>

/*----------------------------------------------------------------*/

/* FIXME: add logging */

/*----------------------------------------------------------------*/

/*
 * This library is not thread-safe.
 */
typedef uint64_t block_index;

struct block_cache;

struct bc_block {
	block_index index;
	void *data;
};

typedef uint64_t sector_t;

struct block_cache *block_cache_create(int fd, sector_t block_size,
				       uint64_t max_nr_blocks, size_t mem);
void block_cache_destroy(struct block_cache *bc);

uint64_t block_cache_get_nr_blocks(struct block_cache *bc);

enum get_flags {
	GF_ZERO = (1 << 0),
	GF_CAN_BLOCK = (1 << 1)
};
struct bc_block *block_cache_get(struct block_cache *bc, block_index index, unsigned flags);

enum put_flags {
	PF_DIRTY = (1 << 0),
};
void block_cache_put(struct bc_block *b, unsigned flags);

/*
 * Flush can fail if an earlier write failed.  You do not know which block
 * failed.  Make sure you build your recovery with this in mind.
 */
int block_cache_flush(struct block_cache *bc);

void block_cache_prefetch(struct block_cache *bc, block_index index);

/*----------------------------------------------------------------*/

#endif

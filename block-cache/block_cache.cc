#include "block-cache/block_cache.h"

#include <assert.h>
#include <libaio.h>
#include <errno.h>
#include <pthread.h>
#include <stdarg.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>

#include <iostream>

//----------------------------------------------------------------

// FIXME: get from linux headers
#define SECTOR_SHIFT 9
#define PAGE_SIZE 4096

#define MIN_BLOCKS 16
#define WRITEBACK_LOW_THRESHOLD_PERCENT 33
#define WRITEBACK_HIGH_THRESHOLD_PERCENT 66

//----------------------------------------------------------------

namespace {
	// FIXME: remove

	/*----------------------------------------------------------------
	 * Logging
	 *--------------------------------------------------------------*/
	void info(const char *format, ...)
	{
		va_list ap;

		va_start(ap, format);
		vfprintf(stderr, format, ap);
		va_end(ap);
	}

	void *alloc_aligned(size_t len, size_t alignment)
	{
		void *result = NULL;
		int r = posix_memalign(&result, alignment, len);
		if (r)
			return NULL;

		return result;
	}
}

//----------------------------------------------------------------

namespace bcache {
	int
	block_cache::init_free_list(unsigned count)
	{
		size_t len;
		block *blocks;
		size_t block_size = block_size_ << SECTOR_SHIFT;
		void *data;
		unsigned i;

		/* Allocate the block structures */
		len = sizeof(block) * count;
		blocks = static_cast<block *>(malloc(len));
		if (!blocks)
			return -ENOMEM;

		blocks_memory_.reset(reinterpret_cast<unsigned char *>(blocks));

		/* Allocate the data for each block.  We page align the data. */
		data = alloc_aligned(count * block_size, PAGE_SIZE);
		if (!data) {
			free(blocks);
			return -ENOMEM;
		}

		blocks_data_.reset(reinterpret_cast<unsigned char *>(data));

		for (i = 0; i < count; i++) {
			block *b = blocks + i;
			INIT_LIST_HEAD(&b->list_);
			b->data_ = data + block_size * i;

			list_add(&b->list_, &free_);
		}

		return 0;
	}

	block_cache::block *
	block_cache::__alloc_block()
	{
		block *b;

		if (list_empty(&free_))
			return NULL;

		b = list_first_entry(&free_, block, list_);
		list_del(&b->list_);

		return b;
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
	void
	block_cache::complete_io(block &b, int result)
	{
		b.error_ = result;
		clear_flags(b, IO_PENDING);
		nr_io_pending_--;

		if (b.error_)
			list_move_tail(&b.list_, &errored_);
		else {
			if (test_flags(b, DIRTY)) {
				clear_flags(b, DIRTY);
				nr_dirty_--;
			}

			list_move_tail(&b.list_, &clean_);
		}
	}

	/*
	 * |b->list| should be valid (either pointing to itself, on one of the other
	 * lists.
	 */
	int
	block_cache::issue_low_level(block &b, enum io_iocb_cmd opcode, const char *desc)
	{
		int r;
		iocb *control_blocks[1];

		assert(!test_flags(b, IO_PENDING));
		set_flags(b, IO_PENDING);
		nr_io_pending_++;
		list_move_tail(&b.list_, &io_pending_);

		b.control_block_.aio_lio_opcode = opcode;
		control_blocks[0] = &b.control_block_;
		r = io_submit(aio_context_, 1, control_blocks);
		if (r != 1) {
			if (r < 0) {
				perror("io_submit error");
				info("io_submit failed with %s op: %d\n", desc, r);
			} else
				info("could not submit IOs, with %s op\n", desc);

			complete_io(b, EIO);
			return -EIO;
		}

		return 0;
	}

	int
	block_cache::issue_read(block &b)
	{
		return issue_low_level(b, IO_CMD_PREAD, "read");
	}

	int
	block_cache::issue_write(block &b)
	{
		std::cerr << "issuing write for block " << b.index_ << "\n";
		return issue_low_level(b, IO_CMD_PWRITE, "write");
	}

	void
	block_cache::wait_io()
	{
		int r;
		unsigned i;

		// FIXME: use a timeout to prevent hanging
		r = io_getevents(aio_context_, 1, nr_cache_blocks_, &events_[0], NULL);
		if (r < 0) {
			info("io_getevents failed %d\n", r);
			exit(1);	/* FIXME: handle more gracefully */
		}

		for (i = 0; i < static_cast<unsigned>(r); i++) {
			io_event const &e = events_[i];
			block *b = container_of(e.obj, block, control_block_);

			if (e.res == block_size_ << SECTOR_SHIFT)
				complete_io(*b, 0);

			else if (e.res < 0)
				complete_io(*b, e.res);

			else
				info("incomplete io, unexpected: %d\n", r);
		}
	}

	/*----------------------------------------------------------------
	 * Clean/dirty list management
	 *--------------------------------------------------------------*/

	/*
	 * We're using lru lists atm, but I think it would be worth
	 * experimenting with a multiqueue approach.
	 */
	list_head *
	block_cache::__categorise(block &b)
	{
		if (b.error_)
			return &errored_;

		return (b.flags_ & DIRTY) ? &dirty_ : &clean_;
	}

	void
	block_cache::hit(block &b)
	{
		list_move_tail(&b.list_, __categorise(b));
	}

	/*----------------------------------------------------------------
	 * High level IO handling
	 *--------------------------------------------------------------*/
	void
	block_cache::wait_all()
	{
		while (!list_empty(&io_pending_))
			wait_io();
	}

	void
	block_cache::wait_specific(block &b)
	{
		while (test_flags(b, IO_PENDING))
			wait_io();
	}

	unsigned
	block_cache::writeback(unsigned count)
	{
		int r;
		block *b, *tmp;
		unsigned actual = 0;

		list_for_each_entry_safe (b, tmp, &dirty_, list_) {
			if (actual == count)
				break;

			if (b->ref_count_)
				continue;

			r = issue_write(*b);
			if (!r)
				actual++;
		}

		info("writeback: requested %u, actual %u\n", count, actual);
		return actual;
	}

	/*----------------------------------------------------------------
	 * Hash table
	 *---------------------------------------------------------------*/

	/*
	 * |nr_buckets| must be a power of two.
	 */
	void
	block_cache::hash_init(unsigned nr_buckets)
	{
		unsigned i;

		nr_buckets_ = nr_buckets;
		mask_ = nr_buckets - 1;

		for (i = 0; i < nr_buckets; i++)
			INIT_LIST_HEAD(&buckets_[i]);
	}

	unsigned
	block_cache::hash(uint64_t index)
	{
		const unsigned BIG_PRIME = 4294967291UL;
		return (((unsigned) index) * BIG_PRIME) & mask_;
	}

	block_cache::block *
	block_cache::hash_lookup(block_index index)
	{
		block *b;
		unsigned bucket = hash(index);

		list_for_each_entry (b, &buckets_[bucket], hash_list_) {
			if (b->index_ == index)
				return b;
		}

		return NULL;
	}

	void
	block_cache::hash_insert(block &b)
	{
		unsigned bucket = hash(b.index_);
		list_move_tail(&b.hash_list_, &buckets_[bucket]);
	}

	void
	block_cache::hash_remove(block &b)
	{
		list_del_init(&b.hash_list_);
	}

	/*----------------------------------------------------------------
	 * High level allocation
	 *--------------------------------------------------------------*/
	void
	block_cache::setup_control_block(block &b)
	{
		iocb *cb = &b.control_block_;
		size_t block_size_bytes = block_size_ << SECTOR_SHIFT;

		memset(cb, 0, sizeof(*cb));
		cb->aio_fildes = fd_;

		cb->u.c.buf = b.data_;
		cb->u.c.offset = block_size_bytes * b.index_;
		cb->u.c.nbytes = block_size_bytes;
	}

	block_cache::block *
	block_cache::new_block(block_index index)
	{
		block *b;

		b = __alloc_block();
		if (!b) {
			if (list_empty(&clean_)) {
				if (list_empty(&io_pending_))
					writeback(9000);
				wait_io();
			}

			if (!list_empty(&clean_)) {
				b = list_first_entry(&clean_, block, list_);
				hash_remove(*b);
				list_del(&b->list_);
			}
		}

		if (b) {
			INIT_LIST_HEAD(&b->list_);
			INIT_LIST_HEAD(&b->hash_list_);
			b->bc_ = this;
			b->ref_count_ = 0;

			b->error_ = 0;
			clear_flags(*b, IO_PENDING | DIRTY);

			b->index_ = index;
			setup_control_block(*b);

			hash_insert(*b);
		}

		return b;
	}

	/*----------------------------------------------------------------
	 * Block reference counting
	 *--------------------------------------------------------------*/
	void
	block_cache::mark_dirty(block &b)
	{
		if (!test_flags(b, DIRTY)) {
			set_flags(b, DIRTY);
			list_move_tail(&b.list_, &dirty_);
			nr_dirty_++;
		}
	}

	unsigned
	block_cache::calc_nr_cache_blocks(size_t mem, sector_t block_size)
	{
		size_t space_per_block = (block_size << SECTOR_SHIFT) + sizeof(block);
		unsigned r = mem / space_per_block;

		return (r < MIN_BLOCKS) ? MIN_BLOCKS : r;
	}

	unsigned
	block_cache::calc_nr_buckets(unsigned nr_blocks)
	{
		unsigned r = 8;
		unsigned n = nr_blocks / 4;

		if (n < 8)
			n = 8;

		while (r < n)
			r <<= 1;

		return r;
	}

	block_cache::block_cache(int fd, sector_t block_size, uint64_t on_disk_blocks, size_t mem)
		: nr_dirty_(0),
		  nr_io_pending_(0)
	{
		int r;
		unsigned nr_cache_blocks = calc_nr_cache_blocks(mem, block_size);
		unsigned nr_buckets = calc_nr_buckets(nr_cache_blocks);

		info("block_size = %llu, on_disk_blocks = %llu, mem = %llu, nr_cache_blocks = %llu\n",
		     (unsigned long long) block_size,
		     (unsigned long long) on_disk_blocks,
		     (unsigned long long) mem,
		     (unsigned long long) nr_cache_blocks);


		buckets_.resize(nr_buckets);

		fd_ = fd;
		block_size_ = block_size;
		nr_data_blocks_ = on_disk_blocks;
		nr_cache_blocks_ = nr_cache_blocks;

		events_.resize(nr_cache_blocks);

		aio_context_ = 0; /* needed or io_setup will fail */
		r = io_setup(nr_cache_blocks, &aio_context_);
		if (r < 0)
			throw std::runtime_error("io_setup failed");

		hash_init(nr_buckets);
		INIT_LIST_HEAD(&free_);
		INIT_LIST_HEAD(&errored_);
		INIT_LIST_HEAD(&dirty_);
		INIT_LIST_HEAD(&clean_);
		INIT_LIST_HEAD(&io_pending_);

		r = init_free_list(nr_cache_blocks);
		if (r)
			throw std::runtime_error("couldn't allocate blocks");
	}

	block_cache::~block_cache()
	{
		wait_all();

		// FIXME: use unique_ptrs
		if (aio_context_)
			io_destroy(aio_context_);
	}

	uint64_t
	block_cache::get_nr_blocks() const
	{
		return nr_data_blocks_;
	}

	void
	block_cache::zero_block(block &b)
	{
		memset(b.data_, 0, block_size_ << SECTOR_SHIFT);
		mark_dirty(b);
	}

	block_cache::block *
	block_cache::lookup_or_read_block(block_index index, unsigned flags)
	{
		block *b = hash_lookup(index);

		if (b) {
			if (test_flags(*b, IO_PENDING))
				wait_specific(*b);

			if (flags & GF_ZERO)
				zero_block(*b);

		} else {
			if (flags & GF_CAN_BLOCK) {
				b = new_block(index);
				if (b) {
					if (flags & GF_ZERO)
						zero_block(*b);
					else {
						issue_read(*b);
						wait_specific(*b);
					}
				}
			}
		}

		return (!b || b->error_) ? NULL : b;
	}

	block_cache::block &
	block_cache::get(block_index index, unsigned flags)
	{
		block *b = lookup_or_read_block(index, flags);

		if (b) {
			hit(*b);
			b->ref_count_++;

			return *b;
		}

		throw std::runtime_error("couldn't get block");
	}

	void
	block_cache::put(block_cache::block &b, unsigned flags)
	{
		if (b.ref_count_ == 0)
			throw std::runtime_error("bad put");

		b.ref_count_--;

		if (flags & PF_DIRTY) {
			mark_dirty(b);

			// FIXME: factor out
			unsigned nr_available = nr_cache_blocks_ - (nr_dirty_ - nr_io_pending_);
			if (nr_available < (WRITEBACK_LOW_THRESHOLD_PERCENT * nr_cache_blocks_ / 100))
				writeback((WRITEBACK_HIGH_THRESHOLD_PERCENT * nr_cache_blocks_ / 100) - nr_available);
		}
	}

	int
	block_cache::flush()
	{
		block *b;

		list_for_each_entry (b, &dirty_, list_) {
			if (b->ref_count_) {
				info("attempt to lock an already locked block\n");
				return -EAGAIN;
			}

			issue_write(*b);
		}

		wait_all();

		return list_empty(&errored_) ? 0 : -EIO;
	}

	void
	block_cache::prefetch(block_index index)
	{
		block *b = hash_lookup(index);

		if (!b) {
			b = new_block(index);
			if (b)
				issue_read(*b);
		}
	}

	//--------------------------------

	unsigned
	block_cache::test_flags(block &b, unsigned flags)
	{
		return b.flags_ & flags;
	}

	void
	block_cache::clear_flags(block &b, unsigned flags)
	{
		b.flags_ &= ~flags;
	}

	void
	block_cache::set_flags(block &b, unsigned flags)
	{
		b.flags_ |= flags;
	}
}

//----------------------------------------------------------------

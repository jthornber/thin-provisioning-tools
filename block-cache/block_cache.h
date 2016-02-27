#ifndef BLOCK_CACHE_H
#define BLOCK_CACHE_H

#include "block-cache/list.h"

#include <boost/shared_ptr.hpp>
#include <boost/noncopyable.hpp>

#include <stdexcept>
#include <libaio.h>
#include <memory>
#include <stdint.h>
#include <stdlib.h>
#include <vector>

//----------------------------------------------------------------

namespace bcache {
	typedef uint64_t block_address;
	typedef uint64_t sector_t;

	class validator {
	public:
		typedef boost::shared_ptr<validator> ptr;

		virtual ~validator() {}

		virtual void check(void const *data, block_address location) const = 0;
		virtual bool check_raw(void const *data) const = 0;
		virtual void prepare(void *data, block_address location) const = 0;
	};

	class noop_validator : public validator {
	public:
		void check(void const *data, block_address location) const {}
		bool check_raw(void const *data) const {return true;}
		void prepare(void *data, block_address location) const {}
	};

	//----------------------------------------------------------------

	class block_cache : private boost::noncopyable {
	public:
		enum block_flags {
			BF_IO_PENDING = (1 << 0),
			BF_DIRTY = (1 << 1),
			BF_FLUSH = (1 << 2),
			BF_PREVIOUSLY_DIRTY = (1 << 3)
		};

		class block : private boost::noncopyable {
		public:
			block()
				: v_() {
				INIT_LIST_HEAD(&list_);
			}

			// Do not give this class a destructor, it wont get
			// called because we manage allocation ourselves.

			uint64_t get_index() const {
				return index_;
			}

			void *get_data() const {
				return data_;
			}

			void mark_dirty() {
				set_flags(BF_DIRTY);
			}

			void set_flags(unsigned flags) {
				flags_ |= flags;
			}

			unsigned test_flags(unsigned flags) const {
				return flags_ & flags;
			}

			void clear_flags(unsigned flags) {
				flags_ &= ~flags;
			}

			void get() {
				ref_count_++;
			};

			void put() {
				if (!ref_count_)
					throw std::runtime_error("bad put");

				if (!--ref_count_)
					bc_->release(*this);
			}

		private:
			friend class block_cache;

			block_cache *bc_;

			uint64_t index_;
			void *data_;

			list_head list_;
			list_head hash_list_;

			unsigned ref_count_;

			int error_;
			unsigned flags_;

			iocb control_block_;
			validator::ptr v_;
		};

		//--------------------------------

		block_cache(int fd, sector_t block_size,
			    uint64_t max_nr_blocks, size_t mem);
		~block_cache();

		uint64_t get_nr_blocks() const;
		uint64_t get_nr_locked() const;

		enum get_flags {
			GF_ZERO = (1 << 0),
			GF_DIRTY = (1 << 1),
			GF_BARRIER = (1 << 2)
		};

		block_cache::block &get(block_address index, unsigned flags, validator::ptr v);

		/*
		 * Flush can fail if an earlier write failed.  You do not know which block
		 * failed.  Make sure you build your recovery with this in mind.
		 */
		int flush();
		void prefetch(block_address index);

	private:
		int init_free_list(unsigned count);
		void exit_free_list();
		block *__alloc_block();
		void complete_io(block &b, int result);
		void issue_low_level(block &b, enum io_iocb_cmd opcode, const char *desc);
		void issue_read(block &b);
		void issue_write(block &b);
		void wait_io();
		list_head *__categorise(block &b);
		void hit(block &b);
		void wait_all();
		void wait_specific(block &b);
		unsigned writeback(unsigned count);
		void hash_init(unsigned nr_buckets);
		unsigned hash(uint64_t index);
		block *hash_lookup(block_address index);
		void hash_insert(block &b);
		void hash_remove(block &b);
		void setup_control_block(block &b);
		block *find_unused_clean_block();
		block *new_block(block_address index);
		void mark_dirty(block &b);
		unsigned calc_nr_cache_blocks(size_t mem, sector_t block_size);
		unsigned calc_nr_buckets(unsigned nr_blocks);
		void zero_block(block &b);
		block *lookup_or_read_block(block_address index, unsigned flags, validator::ptr v);

		void preemptive_writeback();
		void release(block_cache::block &block);
		void check_index(block_address index) const;

		void inc_hit_counter(unsigned flags);
		void inc_miss_counter(unsigned flags);

		//--------------------------------

		int fd_;
		sector_t block_size_;
		uint64_t nr_data_blocks_;
		uint64_t nr_cache_blocks_;

		// We can't use auto_ptr or unique_ptr because the memory is allocated with malloc
		void *blocks_memory_;
		void *blocks_data_;

		io_context_t aio_context_;
		std::vector<io_event> events_;

		/*
		 * Blocks on the free list are not initialised, apart from the
		 * b.data field.
		 */
		list_head free_;
		list_head errored_;
		list_head dirty_;
		list_head clean_;

		unsigned nr_locked_;
		unsigned nr_dirty_;

		unsigned nr_io_pending_;
		struct list_head io_pending_;

		/*
		 * Hash table fields.
		 */
		unsigned nr_buckets_;
		unsigned mask_;
		std::vector<list_head> buckets_;

		// Stats
		unsigned read_hits_;
		unsigned read_misses_;
		unsigned write_zeroes_;
		unsigned write_hits_;
		unsigned write_misses_;
		unsigned prefetches_;

		validator::ptr noop_validator_;
	};
}

//----------------------------------------------------------------

#endif

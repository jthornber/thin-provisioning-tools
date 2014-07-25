#ifndef BLOCK_CACHE_H
#define BLOCK_CACHE_H

#include "block-cache/list.h"

#include <boost/shared_ptr.hpp>
#include <boost/noncopyable.hpp>

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
		virtual void prepare(void *data, block_address location) const = 0;
	};

	class noop_validator : public validator {
	public:
		void check(void const *data, block_address location) const {}
		void prepare(void *data, block_address location) const {}
	};

	//----------------------------------------------------------------

	// FIXME: throw exceptions rather than returning errors
	class block_cache : private boost::noncopyable {
	public:
		enum block_flags {
			IO_PENDING = (1 << 0),
			DIRTY = (1 << 1)
		};

		class block : private boost::noncopyable {
		public:
			block()
				: v_() {
			}

			uint64_t get_index() const {
				return index_;
			}

			void *get_data() const {
				return data_;
			}

		private:
			friend class block_cache;

			uint64_t index_;
			void *data_;

			list_head list_;
			list_head hash_list_;

			block_cache *bc_;
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

		enum get_flags {
			GF_ZERO = (1 << 0),
			GF_CAN_BLOCK = (1 << 1)
		};

		// FIXME: what if !GF_CAN_BLOCK?
		block_cache::block &get(block_address index, unsigned flags, validator::ptr v);

		enum put_flags {
			PF_DIRTY = (1 << 0),
		};

		void put(block_cache::block &block, unsigned flags);

		/*
		 * Flush can fail if an earlier write failed.  You do not know which block
		 * failed.  Make sure you build your recovery with this in mind.
		 */
		int flush();
		void prefetch(block_address index);

	private:
		int init_free_list(unsigned count);
		block *__alloc_block();
		void complete_io(block &b, int result);
		int issue_low_level(block &b, enum io_iocb_cmd opcode, const char *desc);
		int issue_read(block &b);
		int issue_write(block &b);
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
		block *new_block(block_address index);
		void mark_dirty(block &b);
		unsigned calc_nr_cache_blocks(size_t mem, sector_t block_size);
		unsigned calc_nr_buckets(unsigned nr_blocks);
		void zero_block(block &b);
		block *lookup_or_read_block(block_address index, unsigned flags, validator::ptr v);
		unsigned test_flags(block &b, unsigned flags);
		void clear_flags(block &b, unsigned flags);
		void set_flags(block &b, unsigned flags);

		//--------------------------------

		int fd_;
		sector_t block_size_;
		uint64_t nr_data_blocks_;
		uint64_t nr_cache_blocks_;

		std::auto_ptr<unsigned char> blocks_memory_; // FIXME: change to a vector
		std::auto_ptr<unsigned char> blocks_data_;

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

		unsigned nr_dirty_;

		unsigned nr_io_pending_;
		struct list_head io_pending_;

		/*
		 * Hash table fields.
		 */
		unsigned nr_buckets_;
		unsigned mask_;
		std::vector<list_head> buckets_;
	};

#if 0
	class auto_lock {
	public:
		auto_lock(block_cache &bc, block_address index, bool zero, validator::ptr v, unsigned put_flags)
			: bc_(bc),
			  b_(bc.get(index, (zero ? block_cache::GF_ZERO : 0) | block_cache::GF_CAN_BLOCK, v)),
			  put_flags_(put_flags),
			  holders_(new unsigned) {
			*holders_ = 1;
		}

		virtual ~auto_lock() {
			bc_.put(b_, put_flags_);
		}

		auto_lock operator =(auto_lock const &rhs) {
			if (this != &rhs) {
				bc_ = rhs.bc_;
				
			

		void const *data() const {
			return b_.get_data();
		}

	private:
		block_cache &bc_;
		block_cache::block &b_;
		unsigned put_flags_;
		unsigned *holders_;
	};

	class auto_read_lock : public auto_lock {
	public:
		auto_read_lock(block_cache &bc, block_address index, bool zero, validator::ptr v)
			: auto_lock(bc, index, zero, v, 0) {
		}

		using auto_lock::data();
	};

	class auto_write_lock : public auto_lock {
	public:
		auto_write_lock(block_cache &bc, block_address index, bool zero, validator::ptr v)
			: auto_lock(bc, index, zero, v, block_cache::DIRTY) {
		}

		using auto_lock::data();
		void *data() {
			return b_.get_data();
		}
	};
#endif
}

//----------------------------------------------------------------

#endif

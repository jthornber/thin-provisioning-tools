#ifndef BLOCK_CACHE_H
#define BLOCK_CACHE_H

#include <boost/intrusive/list.hpp>
#include <boost/intrusive/set.hpp>
#include <boost/noncopyable.hpp>
#include <boost/shared_ptr.hpp>
#include <functional>
#include <iostream>
#include <libaio.h>
#include <memory>
#include <stdexcept>
#include <stdint.h>
#include <stdlib.h>
#include <vector>

namespace bi = boost::intrusive;

//----------------------------------------------------------------

// FIXME: move to own file in base
template<class P, class M>
size_t offsetof__(const M P::*member)
{
	return (size_t) &( reinterpret_cast<P*>(0)->*member);
}

template<class P, class M>
P *container_of(M *ptr, M const P::*member)
{
	return (P *)((char *)(ptr) - offsetof__(member));
}

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
			}

			bool operator <(block const &rhs) const {
				return index_ > rhs.index_;
			}

			bool operator ==(block const &rhs) const {
				return index_ == rhs.index_;
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

			void unlink() {
				list_hook_.unlink();
			}

		private:
			friend class block_cache;
			friend class cmp_index;

			block_cache *bc_;

			uint64_t index_;
			void *data_;

			bi::list_member_hook<bi::link_mode<bi::auto_unlink>> list_hook_;
			bi::set_member_hook<> set_hook_;

			unsigned ref_count_;

			int error_;
			unsigned flags_;

			iocb control_block_;
			validator::ptr v_;
		};

		struct cmp_index {
			bool operator()(block_address index, block const &b) const {
				return index > b.index_;
			}

			bool operator()(block const &b, block_address index) const {
				return b.index_ > index;
			}
		};

		class auto_block {
		public:
			auto_block()
				: b_(0) {
			}

			auto_block(block &b)
			: b_(&b) {
			}

			~auto_block() {
				put();
			}

			auto_block &operator =(block &b) {
				put();
				b_ = &b;
				return *this;
			}

			void *get_data() const {
				if (b_)
					return b_->get_data();

				throw std::runtime_error("auto_block not set");
			}

		private:
			void put() {
				if (b_) {
					b_->put();
					b_ = 0;
				}
			}

			block *b_;
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
		typedef bi::member_hook<block,
					bi::list_member_hook<bi::link_mode<bi::auto_unlink>>,
					&block::list_hook_> list_hook_option;
		typedef bi::list<block, list_hook_option,
					bi::constant_time_size<false>> block_list;

		int init_free_list(unsigned count);
		block *__alloc_block();
		void complete_io(block &b, int result);
		void issue_low_level(block &b, enum io_iocb_cmd opcode, const char *desc);
		void issue_read(block &b);
		void issue_write(block &b);
		void wait_io();
		block_list &__categorise(block &b);
		void hit(block &b);
		void wait_all();
		void wait_specific(block &b);
		unsigned writeback(unsigned count);
		void setup_control_block(block &b);
		block *find_unused_clean_block();
		block *new_block(block_address index);
		void mark_dirty(block &b);
		unsigned calc_nr_cache_blocks(size_t mem, sector_t block_size);
		unsigned calc_nr_buckets(unsigned nr_blocks);
		void zero_block(block &b);
		block *lookup_or_read_block(block_address index, unsigned flags, validator::ptr v);
		void exit_free_list();

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

		std::unique_ptr<std::vector<block>> blocks_memory_;
		unsigned char *blocks_data_;

		io_context_t aio_context_;
		std::vector<io_event> events_;

		/*
		 * Blocks on the free list are not initialised, apart from the
		 * b.data field.
		 */
		block_list free_;
		block_list errored_;
		block_list dirty_;
		block_list clean_;

		unsigned nr_locked_;
		unsigned nr_dirty_;

		unsigned nr_io_pending_;
		block_list io_pending_;

		typedef bi::member_hook<block,
					bi::set_member_hook<>,
					&block::set_hook_> block_option;
		typedef bi::set<block, block_option,
				bi::constant_time_size<false>> block_set;
		block_set block_set_;

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

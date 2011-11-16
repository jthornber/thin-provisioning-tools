#ifndef SPACE_MAP_H
#define SPACE_MAP_H

#include "block.h"
#include "block_counter.h"

#include <boost/shared_ptr.hpp>
#include <boost/optional.hpp>

//----------------------------------------------------------------

namespace persistent_data {
	typedef uint32_t ref_t;

	class space_map {
	public:
		typedef boost::shared_ptr<space_map> ptr;

		virtual ~space_map() {};

		virtual block_address get_nr_blocks() const = 0;
		virtual block_address get_nr_free() const = 0;
		virtual ref_t get_count(block_address b) const = 0;
		virtual void set_count(block_address b, ref_t c) = 0;
		virtual void commit() = 0;

		virtual void inc(block_address b) = 0;
		virtual void dec(block_address b) = 0;

		// FIXME: change these to return an optional, failure is
		// not that rare if we're restricting the area that's
		// searched.
		typedef boost::optional<block_address> maybe_block;

		virtual maybe_block new_block() = 0;
		virtual maybe_block new_block(block_address begin, block_address end) = 0;

		virtual bool count_possibly_greater_than_one(block_address b) const = 0;

		virtual void extend(block_address extra_blocks) = 0;

		struct iterator {
			virtual ~iterator() {}

			virtual void operator() (block_address b, ref_t c) = 0;
		};

		virtual void iterate(iterator &it) const {
			throw std::runtime_error("not implemented");
		}
	};

	class persistent_space_map : public space_map {
	public:
		typedef boost::shared_ptr<persistent_space_map> ptr;

		// FIXME: these two should be const
		virtual size_t root_size() = 0;
		virtual void copy_root(void *dest, size_t len) = 0;
	};

	class checked_space_map : public persistent_space_map {
	public:
		typedef boost::shared_ptr<checked_space_map> ptr;

		virtual void check(block_counter &counter) const {
			throw std::runtime_error("not implemented");
		}

		virtual ptr clone() const = 0;
	};

	class sm_adjust {
	public:
		sm_adjust(space_map::ptr sm, block_address b, int delta)
			: sm_(sm),
			  b_(b),
			  delta_(delta) {

			adjust_count(delta_);
		}

		~sm_adjust() {
			adjust_count(-delta_);
		}

		void release() {
			delta_ = 0;
		}

	private:
		void adjust_count(int delta) {
			if (delta == 1)
				sm_->inc(b_);

			else if (delta == -1)
				sm_->dec(b_);

			else
				sm_->set_count(b_, sm_->get_count(b_) + delta);
		}

		space_map::ptr sm_;
		block_address b_;
		int delta_;
	};

	class sm_decrementer {
	public:
		sm_decrementer(space_map::ptr sm, block_address b);
		~sm_decrementer();
		void dont_bother();

	private:
		space_map::ptr sm_;
		block_address b_;
		bool released_;
	};
}

//----------------------------------------------------------------

#endif

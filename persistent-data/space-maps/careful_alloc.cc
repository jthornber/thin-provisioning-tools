// Copyright (C) 2011 Red Hat, Inc. All rights reserved.
//
// This file is part of the thin-provisioning-tools source.
//
// thin-provisioning-tools is free software: you can redistribute it
// and/or modify it under the terms of the GNU General Public License
// as published by the Free Software Foundation, either version 3 of
// the License, or (at your option) any later version.
//
// thin-provisioning-tools is distributed in the hope that it will be
// useful, but WITHOUT ANY WARRANTY; without even the implied warranty
// of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License along
// with thin-provisioning-tools.  If not, see
// <http://www.gnu.org/licenses/>.

#include "persistent-data/space-maps/careful_alloc.h"
#include "persistent-data/space-maps/subtracting_span_iterator.h"

#include <boost/shared_ptr.hpp>

//----------------------------------------------------------------

namespace {
	using namespace persistent_data;

	class sm_careful_alloc : public checked_space_map {
	public:
		typedef boost::shared_ptr<sm_careful_alloc> ptr;

		sm_careful_alloc(checked_space_map::ptr sm)
			: sm_(sm) {
		}

		virtual block_address get_nr_blocks() const {
			return sm_->get_nr_blocks();
		}

		virtual block_address get_nr_free() const {
			return sm_->get_nr_free();
		}

		virtual ref_t get_count(block_address b) const {
			return sm_->get_count(b);
		}

		virtual void set_count(block_address b, ref_t c) {
			if (!c && sm_->get_count(b))
				mark_freed(b);

			sm_->set_count(b, c);
		}

		virtual void commit() {
			sm_->commit();
			clear_freed();
		}

		virtual void inc(block_address b) {
			if (was_freed(b))
				throw runtime_error("inc of block freed within current transaction");

			sm_->inc(b);
		}

		virtual void dec(block_address b) {
			sm_->dec(b);

			if (!sm_->get_count(b))
				mark_freed(b);
		}

		virtual maybe_block find_free(span_iterator &it) {
			subtracting_span_iterator filtered_it(get_nr_blocks(), it, freed_blocks_);
			return sm_->find_free(filtered_it);
		}

		virtual bool count_possibly_greater_than_one(block_address b) const {
			return sm_->count_possibly_greater_than_one(b);
		}

		virtual void extend(block_address extra_blocks) {
			return sm_->extend(extra_blocks);
		}

		virtual void iterate(iterator &it) const {
			sm_->iterate(it);
		}

		virtual size_t root_size() const {
			return sm_->root_size();
		}

		virtual void copy_root(void *dest, size_t len) const {
			return sm_->copy_root(dest, len);
		}

		virtual void check(block_counter &counter) const {
			return sm_->check(counter);
		}

		virtual checked_space_map::ptr clone() const {
			return checked_space_map::ptr(new sm_careful_alloc(sm_));
		}

	private:
		void clear_freed() {
			freed_blocks_.clear();
		}

		void mark_freed(block_address b) {
			freed_blocks_.add(b, b + 1);
		}

		bool was_freed(block_address b) const {
			return freed_blocks_.member(b) > 0;
		}

		checked_space_map::ptr sm_;
		subtracting_span_iterator::block_set freed_blocks_;
	};
}

//----------------------------------------------------------------

checked_space_map::ptr
persistent_data::create_careful_alloc_sm(checked_space_map::ptr sm)
{
	return checked_space_map::ptr(new sm_careful_alloc(sm));
}


//----------------------------------------------------------------

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

#include "space_map_transactional.h"

//----------------------------------------------------------------

namespace {
	class sm_transactional : public checked_space_map {
	public:
		typedef shared_ptr<sm_transactional> ptr;

		sm_transactional(checked_space_map::ptr sm)
			: sm_(sm),
			  committed_(sm_->clone()),
			  allocated_(0),
			  search_start_(0) {
		}

		virtual block_address get_nr_blocks() const {
			return committed_->get_nr_blocks();
		}

		virtual block_address get_nr_free() const {
			return committed_->get_nr_free() - allocated_;
		}

		virtual ref_t get_count(block_address b) const {
			return sm_->get_count(b);
		}

		virtual void set_count(block_address b, ref_t c) {
			sm_->set_count(b, c);
		}

		virtual void commit() {
			sm_->commit();
			committed_ = sm_->clone();
			allocated_ = 0;
			search_start_ = 0;
		}

		virtual void inc(block_address b) {
			// FIXME: this may do an implicit allocation, so
			// search_start_ and allocated_ will be wrong.
			sm_->inc(b);
		}

		virtual void dec(block_address b) {
			sm_->dec(b);
		}

		virtual maybe_block new_block() {
			return new_block(0, sm_->get_nr_blocks());
		}

		virtual maybe_block new_block(block_address begin, block_address end) {
			if (end <= search_start_)
				return maybe_block();

			maybe_block mb = committed_->new_block(max(search_start_, begin), end);
			if (mb) {
				allocated_++;
				search_start_ = *mb + 1;
			} else
				search_start_ = end;

			return mb;
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
			return checked_space_map::ptr(new sm_transactional(sm_));
		}

	private:
		checked_space_map::ptr sm_;
		checked_space_map::ptr committed_;
		block_address allocated_;
		block_address search_start_;
	};
}

//----------------------------------------------------------------

checked_space_map::ptr
persistent_data::create_transactional_sm(checked_space_map::ptr sm)
{
	return checked_space_map::ptr(new sm_transactional(sm));
}


//----------------------------------------------------------------

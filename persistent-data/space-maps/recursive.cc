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

#include "persistent-data/space-maps/recursive.h"
#include "persistent-data/space-maps/subtracting_span_iterator.h"

#include <list>

using namespace persistent_data;
using namespace std;

//----------------------------------------------------------------

namespace {
	enum op {
		INC,
		SET
	};

	struct block_op {
		block_op(op o, uint32_t rc)
			: op_(o),
			  rc_(rc) {
		}

		op op_;

		// I'm assuming the ref counts never get above 2^31, which is reasonable I think :)
		int32_t rc_;
	};

	// Aggregates two block_ops
	block_op operator +(block_op const &lhs, block_op const &rhs) {
		switch (lhs.op_) {
		case INC:
			switch (rhs.op_) {
			case INC:
				return block_op(INC, lhs.rc_ + rhs.rc_);

			case SET:
				return rhs;
			}
			break;

		case SET:
			switch (rhs.op_) {
			case INC:
				return block_op(SET, lhs.rc_ + rhs.rc_);

			case SET:
				return rhs;
			}
			break;
		}

		throw runtime_error("can't get here");
	}

	class sm_recursive : public checked_space_map {
	public:
		sm_recursive(checked_space_map::ptr sm)
			: sm_(sm),
			  depth_(0),
			  flush_in_progress_(false) {
		}

		virtual block_address get_nr_blocks() const {
			return sm_->get_nr_blocks();
		}

		virtual block_address get_nr_free() const {
			return sm_->get_nr_free();
		}

		virtual ref_t get_count(block_address b) const {
			recursing_const_lock lock(*this);
			return modify_count(b, sm_->get_count(b));
		}

		virtual void set_count(block_address b, ref_t c) {
			if (depth_)
				add_op(b, block_op(SET, c));
			else {
				recursing_lock lock(*this);
				return sm_->set_count(b, c);
			}
		}

		virtual void commit() {
			cant_recurse("commit");
			sm_->commit();
		}

		virtual void inc(block_address b, ref_t count) override {
			if (depth_)
				add_op(b, block_op(INC, count));
			else {
				recursing_lock lock(*this);
				return sm_->inc(b, count);
			}
		}

		virtual void dec(block_address b, ref_t count) override {
			if (depth_)
				add_op(b, block_op(INC, -count));
			else {
				recursing_lock lock(*this);
				return sm_->dec(b, count);
			}
		}

		virtual maybe_block
		find_free(span_iterator &it) {
			recursing_lock lock(*this);

			subtracting_span_iterator filtered_it(get_nr_blocks(), it, allocated_blocks_);
			return sm_->find_free(filtered_it);
		}

		virtual bool count_possibly_greater_than_one(block_address b) const {
			recursing_const_lock lock(*this);

			bool gto = sm_->count_possibly_greater_than_one(b);
			if (gto)
				return true;

			return modify_count(b, 1) > 1;
		}

		virtual void extend(block_address extra_blocks) {
			cant_recurse("extend");
			recursing_lock lock(*this);
			return sm_->extend(extra_blocks);
		}

		virtual void iterate(iterator &it) const {
			sm_->iterate(it);
		}

		virtual void count_metadata(block_counter &bc) const {
			sm_->count_metadata(bc);
		}

		virtual size_t root_size() const {
			cant_recurse("root_size");
			recursing_const_lock lock(*this);
			return sm_->root_size();
		}

		virtual void copy_root(void *dest, size_t len) const {
			cant_recurse("copy_root");
			recursing_const_lock lock(*this);
			return sm_->copy_root(dest, len);
		}

		virtual void visit(space_map_detail::visitor &v) const {
			cant_recurse("check");
			recursing_const_lock lock(*this);
			return sm_->visit(v);
		}

		virtual checked_space_map::ptr clone() const {
			return checked_space_map::ptr(new sm_recursive(sm_->clone()));
		}

		void flush_ops() {
			if (flush_in_progress_)
				return;

			flush_in_progress_ = true;
			flush_ops_();
			flush_in_progress_ = false;
		}

	private:
		uint32_t modify_count(block_address b, uint32_t count) const {
			auto ops_it = ops_.find(b);
			if (ops_it != ops_.end()) {
				auto const &op = ops_it->second;

				switch (op.op_) {
				case INC:
					count += op.rc_;
					break;

				case SET:
					count = op.rc_;
					break;
				}
			}

			return count;
		}

		void flush_ops_() {
			recursing_lock lock(*this);

			for (auto const &p : ops_) {
				block_address b = p.first;
				auto const &op = p.second;

				switch (op.op_) {
				case INC:
					sm_->inc(b, op.rc_);
					break;

				case SET:
					sm_->set_count(b, op.rc_);
					break;
				}
			}

			ops_.clear();
			allocated_blocks_.clear();
		}

		void add_op(block_address b, block_op const &op) {
			auto it = ops_.find(b);

			if (it == ops_.end())
				ops_.insert(make_pair(b, op));
			else
				it->second = it->second + op;

			// FIXME: is this the best we can do? 
			if (op.rc_ > 0)
				allocated_blocks_.add(b, b + 1);
		}

		void cant_recurse(string const &method) const {
			if (depth_)
				throw runtime_error("recursive '" + method + "' not supported");
		}

		struct recursing_lock {
			recursing_lock(sm_recursive &smr)
				: smr_(smr) {
				smr_.depth_++;
			}

			~recursing_lock() {
				if (!--smr_.depth_)
					smr_.flush_ops();
			}

		private:
			sm_recursive &smr_;
		};

		struct recursing_const_lock {
			recursing_const_lock(sm_recursive const &smr)
				: smr_(smr) {
				smr_.depth_++;
			}

			~recursing_const_lock() {
				smr_.depth_--;
			}

		private:
			sm_recursive const &smr_;
		};

		checked_space_map::ptr sm_;
		mutable int depth_;

		map<block_address, block_op> ops_;

		subtracting_span_iterator::block_set allocated_blocks_;
		bool flush_in_progress_;
	};
}

//----------------------------------------------------------------

checked_space_map::ptr
persistent_data::create_recursive_sm(checked_space_map::ptr sm)
{
	return checked_space_map::ptr(new sm_recursive(sm));
}

//----------------------------------------------------------------

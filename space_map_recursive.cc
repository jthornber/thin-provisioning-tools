// Copyright (C) 20011 Red Hat, Inc. All rights reserved.
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

#include "space_map_recursive.h"

using namespace persistent_data;

//----------------------------------------------------------------

namespace {
	struct block_op {
		enum op {
			INC,
			DEC,
			SET
		};

		block_op(op o, block_address b)
			: op_(o),
			  b_(b) {
			if (o == SET)
				throw runtime_error("SET must take an operand");
		}

		block_op(op o, block_address b, uint32_t rc)
			: op_(o),
			  b_(b),
			  rc_(rc) {
			if (o != SET)
				throw runtime_error("only SET takes an operand");
		}

		op op_;
		block_address b_;
		uint32_t rc_;
	};

	class sm_recursive : public checked_space_map {
	public:
		sm_recursive(checked_space_map::ptr sm)
			: sm_(sm),
			  depth_(0) {
		}

		virtual block_address get_nr_blocks() const {
			return sm_->get_nr_blocks();
		}

		virtual block_address get_nr_free() const {
			return sm_->get_nr_free();
		}

		virtual ref_t get_count(block_address b) const {
			cant_recurse("get_count");
			recursing_const_lock lock(*this);
			return sm_->get_count(b);
		}

		virtual void set_count(block_address b, ref_t c) {
			if (depth_)
				add_op(block_op(block_op::SET, b, c));
			else {
				recursing_lock lock(*this);
				return sm_->set_count(b, c);
			}
		}

		virtual void commit() {
			cant_recurse("commit");
			sm_->commit();
		}

		virtual void inc(block_address b) {
			if (depth_)
				add_op(block_op(block_op::INC, b));
			else {
				recursing_lock lock(*this);
				return sm_->inc(b);
			}
		}

		virtual void dec(block_address b) {
			if (depth_)
				add_op(block_op(block_op::DEC, b));
			else {
				recursing_lock lock(*this);
				return sm_->dec(b);
			}
		}

		// new_block must not recurse.
		virtual boost::optional<block_address>
		new_block() {
			cant_recurse("new_block");
			recursing_lock lock(*this);
			return sm_->new_block();
		}

		virtual boost::optional<block_address>
		new_block(block_address begin, block_address end) {
			cant_recurse("new_block(range)");
			recursing_lock lock(*this);
			return sm_->new_block(begin, end);
		}

		virtual bool count_possibly_greater_than_one(block_address b) const {
			if (depth_)
				return true;

			else {
				recursing_const_lock lock(*this);
				return sm_->count_possibly_greater_than_one(b);
			}
		}

		virtual void extend(block_address extra_blocks) {
			cant_recurse("extend");
			recursing_lock lock(*this);
			return sm_->extend(extra_blocks);
		}

		virtual void iterate(iterator &it) const {
			sm_->iterate(it);
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

		virtual void check(persistent_data::block_counter &counter) const {
			cant_recurse("check");
			recursing_const_lock lock(*this);
			return sm_->check(counter);
		}

		virtual checked_space_map::ptr clone() const {
			return checked_space_map::ptr(new sm_recursive(sm_->clone()));
		}

		void flush_ops() {
			op_map::const_iterator it, end = ops_.end();
			for (it = ops_.begin(); it != end; ++it) {
				list<block_op> const &ops = it->second;
				list<block_op>::const_iterator op_it, op_end = ops.end();
				for (op_it = ops.begin(); op_it != op_end; ++op_it) {
					recursing_lock lock(*this);
					switch (op_it->op_) {
					case block_op::INC:
						sm_->inc(op_it->b_);
						break;

					case block_op::DEC:
						sm_->dec(op_it->b_);
						break;

					case block_op::SET:
						sm_->set_count(op_it->b_, op_it->rc_);
						break;
					}
				}
			}

			ops_.clear();
		}

	private:
		void add_op(block_op const &op) {
			ops_[op.b_].push_back(op);
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

		enum op {
			BOP_INC,
			BOP_DEC,
			BOP_SET
		};

		typedef map<block_address, list<block_op> > op_map;
		op_map ops_;
	};
}

//----------------------------------------------------------------

checked_space_map::ptr
persistent_data::create_recursive_sm(checked_space_map::ptr sm)
{
	return checked_space_map::ptr(new sm_recursive(sm));
}

//----------------------------------------------------------------

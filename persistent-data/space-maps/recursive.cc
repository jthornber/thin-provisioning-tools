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
			ref_t count = sm_->get_count(b);

			op_map::const_iterator ops_it = ops_.find(b);
			if (ops_it != ops_.end()) {
				list<block_op>::const_iterator it, end = ops_it->second.end();
				for (it = ops_it->second.begin(); it != end; ++it) {
					switch (it->op_) {
					case block_op::INC:
						count++;
						break;

					case block_op::DEC:
						count--;
						break;

					case block_op::SET:
						count = it->b_;
						break;
					}
				}
			}

			return count;
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

		virtual maybe_block
		find_free(span_iterator &it) {
			recursing_lock lock(*this);

			subtracting_span_iterator filtered_it(get_nr_blocks(), it, allocated_blocks_);
			return sm_->find_free(filtered_it);
		}

		virtual bool count_possibly_greater_than_one(block_address b) const {
			recursing_const_lock lock(*this);
			bool gto = sm_->count_possibly_greater_than_one(b);
			if (!gto) {
				ref_t count = 1;

				// FIXME: duplication
				op_map::const_iterator ops_it = ops_.find(b);
				if (ops_it != ops_.end()) {
					list<block_op>::const_iterator it, end = ops_it->second.end();
					for (it = ops_it->second.begin(); it != end; ++it) {
						switch (it->op_) {
						case block_op::INC:
							count++;
							break;

						case block_op::DEC:
							count--;
							break;

						case block_op::SET:
							count = it->b_;
							break;
						}
					}
				}

				gto = count > 1;
			}

			return gto;
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
		void flush_ops_() {
			op_map::const_iterator it, end = ops_.end();
			for (it = ops_.begin(); it != end; ++it) {
				recursing_lock lock(*this);

				list<block_op> const &ops = it->second;
				list<block_op>::const_iterator op_it, op_end = ops.end();
				for (op_it = ops.begin(); op_it != op_end; ++op_it) {
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
			allocated_blocks_.clear();
		}

		void add_op(block_op const &op) {
			ops_[op.b_].push_back(op);

			if (op.op_ == block_op::INC || (op.op_ == block_op::SET && op.rc_ > 0))
				allocated_blocks_.add(op.b_, op.b_ + 1);
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

		typedef map<block_address, std::list<block_op> > op_map;
		op_map ops_;

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

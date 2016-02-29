// Copyright (C) 2012 Red Hat, Inc. All rights reserved.
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

#ifndef ARRAY_H
#define ARRAY_H

#include "persistent-data/math_utils.h"
#include "persistent-data/data-structures/btree.h"
#include "persistent-data/data-structures/btree_counter.h"
#include "persistent-data/data-structures/btree_damage_visitor.h"
#include "persistent-data/data-structures/array_block.h"

//----------------------------------------------------------------

namespace persistent_data {
	namespace array_detail {
		uint32_t const ARRAY_CSUM_XOR = 595846735;

		struct array_block_validator : public bcache::validator {
			virtual void check(void const *raw, block_address location) const {
				array_block_disk const *data = reinterpret_cast<array_block_disk const *>(raw);
				crc32c sum(ARRAY_CSUM_XOR);
				sum.append(&data->max_entries, MD_BLOCK_SIZE - sizeof(uint32_t));
				if (sum.get_sum() != to_cpu<uint32_t>(data->csum))
					throw checksum_error("bad checksum in array block node");

				if (to_cpu<uint64_t>(data->blocknr) != location)
					throw checksum_error("bad block nr in array block");
			}

			virtual bool check_raw(void const *raw) const {
				array_block_disk const *data = reinterpret_cast<array_block_disk const *>(raw);
				crc32c sum(ARRAY_CSUM_XOR);
				sum.append(&data->max_entries, MD_BLOCK_SIZE - sizeof(uint32_t));
				if (sum.get_sum() != to_cpu<uint32_t>(data->csum))
					return false;
				return true;
			}

			virtual void prepare(void *raw, block_address location) const {
				array_block_disk *data = reinterpret_cast<array_block_disk *>(raw);
				data->blocknr = to_disk<base::le64, uint64_t>(location);

				crc32c sum(ARRAY_CSUM_XOR);
				sum.append(&data->max_entries, MD_BLOCK_SIZE - sizeof(uint32_t));
				data->csum = to_disk<base::le32>(sum.get_sum());
			}
		};

		struct array_dim {
			array_dim(unsigned nr_entries, unsigned entries_per_block)
				: nr_full_blocks(nr_entries / entries_per_block),
				  nr_entries_in_last_block(nr_entries % entries_per_block),
				  nr_total_blocks(nr_full_blocks + (nr_entries_in_last_block ? 1 : 0)) {
			}

			unsigned nr_full_blocks;
			unsigned nr_entries_in_last_block;
			unsigned nr_total_blocks;
		};

		struct damage {
			typedef boost::shared_ptr<damage> ptr;

			damage(run<uint32_t> lost_keys,
			       std::string const &desc)
				: lost_keys_(lost_keys),
				  desc_(desc) {
			}

			run<uint32_t> lost_keys_;
			std::string desc_;
		};

		inline std::ostream &operator <<(std::ostream &out, damage const &d) {
			out << "array damage[lost_keys = " << d.lost_keys_
			    << ", \"" << d.desc_ << "\"]";
			return out;
		}
	}

	class array_base {
	public:
		virtual ~array_base() {}
		virtual void set_root(block_address root) = 0;
		virtual block_address get_root() const = 0;
	};

	template <typename ValueTraits>
	class array : public array_base {
	public:
		class block_ref_counter : public ref_counter<uint64_t> {
		public:
			block_ref_counter(space_map::ptr sm,
					  array<ValueTraits> &a)
				: sm_(sm),
				  a_(a) {
			}

			virtual void set(uint64_t b, uint32_t rc) {
				sm_->set_count(b, rc);
				if (rc == 0)
					dec_values(b);
			}

			virtual void inc(uint64_t b) {
				sm_->inc(b);
			}

			virtual void dec(uint64_t b) {
				sm_->dec(b);
				if (sm_->get_count(b) == 0)
					dec_values(b);
			}

		private:
			void dec_values(uint64_t b) {
				a_.dec_ablock_entries(b);
			}

			space_map::ptr sm_;
			array<ValueTraits> &a_;
		};
		friend class block_ref_counter;

		struct block_traits {
			typedef base::le64 disk_type;
			typedef block_address value_type;
			typedef block_ref_counter ref_counter;

			static void unpack(disk_type const &disk, value_type &value) {
				value = base::to_cpu<uint64_t>(disk);
			}

			static void pack(value_type const &value, disk_type &disk) {
				disk = base::to_disk<base::le64>(value);
			}
		};

		template <typename ValueVisitor>
		struct block_value_visitor {
			block_value_visitor(array<ValueTraits> const &a, ValueVisitor &vv)
				: a_(a),
				  vv_(vv),
				  highest_index_() {
			}

			void visit(btree_path const &p,
				   typename block_traits::value_type const &block) {
				highest_index_ = max<unsigned>(highest_index_,
							       a_.visit_array_block(vv_, p, block));
			}

			unsigned get_highest_seen() const {
				return highest_index_;
			}

		private:
			array<ValueTraits> const &a_;
			ValueVisitor &vv_;
			unsigned highest_index_;
		};

		// Returns the highest index visited
		template <typename ValueVisitor>
		unsigned visit_array_block(ValueVisitor &vv,
					   btree_path const &p,
					   typename block_traits::value_type const &v) const {
			rblock rb(tm_.read_lock(v, validator_), rc_);

			for (uint32_t i = 0; i < rb.nr_entries(); i++)
				vv.visit(p[0] * rb.max_entries() + i, rb.get(i));

			return p[0] * rb.max_entries() + (rb.nr_entries() - 1);
		}

		template <typename DamageVisitor>
		struct block_damage_visitor {
			block_damage_visitor(DamageVisitor &dv, unsigned entries_per_block)
				: dv_(dv),
				  entries_per_block_(entries_per_block) {
			}

			void visit(btree_path const &path, btree_detail::damage const &d) {
				dv_.visit(array_detail::damage(convert_run(d.lost_keys_), d.desc_));
			}

		private:
			run<uint32_t>::maybe convert_maybe(run<uint64_t>::maybe const &v) const {
				if (v)
					return run<uint32_t>::maybe(*v * entries_per_block_);

				return run<uint32_t>::maybe();
			}

			run<uint32_t> convert_run(run<uint64_t> const &v) const {
				return run<uint32_t>(convert_maybe(v.begin_), convert_maybe(v.end_));
			}

			DamageVisitor &dv_;
			unsigned entries_per_block_;
		};

		typedef block_manager<>::write_ref write_ref;
		typedef block_manager<>::read_ref read_ref;

		typedef array_block<ValueTraits, block_manager<>::write_ref> wblock;
		typedef array_block<ValueTraits, block_manager<>::read_ref> rblock;

		typedef boost::shared_ptr<array<ValueTraits> > ptr;
		typedef typename ValueTraits::value_type value_type;
		typedef typename ValueTraits::ref_counter ref_counter;

		array(transaction_manager &tm, ref_counter rc)
			: tm_(tm),
			  entries_per_block_(rblock::calc_max_entries()),
			  nr_entries_(0),
			  block_rc_(tm.get_sm(), *this),
			  block_tree_(tm, block_rc_),
			  rc_(rc),
			  validator_(new array_detail::array_block_validator) {
		}

		array(transaction_manager &tm, ref_counter rc,
		      block_address root,
		      unsigned nr_entries)
			: tm_(tm),
			  entries_per_block_(rblock::calc_max_entries()),
			  nr_entries_(nr_entries),
			  block_rc_(tm.get_sm(), *this),
			  block_tree_(tm, root, block_rc_),
			  rc_(rc),
			  validator_(new array_detail::array_block_validator) {
		}

		unsigned get_nr_entries() const {
			return nr_entries_;
		}

		// FIXME: why is this needed?
		void set_root(block_address root) {
			block_tree_.set_root(root);
		}

		block_address get_root() const {
			return block_tree_.get_root();
		}

		void destroy() {
			block_tree_.destroy(); // FIXME: not implemented
		}

		void grow(unsigned new_nr_entries, value_type const &v) {
			resizer r(*this, nr_entries_, new_nr_entries, entries_per_block_, v);
			r.grow(new_nr_entries, v);
		}

		value_type get(unsigned index) const {
			rblock b = get_ablock(index / entries_per_block_);
			return b.get(index % entries_per_block_);
		}

		void set(unsigned index, value_type const &value) {
			wblock b = shadow_ablock(index / entries_per_block_);
			b.set(index % entries_per_block_, value);
		}

		template <typename ValueVisitor, typename DamageVisitor>
		void visit_values(ValueVisitor &value_visitor,
				  DamageVisitor &damage_visitor) const {
			block_value_visitor<ValueVisitor> bvisitor(*this, value_visitor);
			block_damage_visitor<DamageVisitor> dvisitor(damage_visitor, entries_per_block_);
			btree_visit_values(block_tree_, bvisitor, dvisitor);

			// check that all blocks were seen
			unsigned h = bvisitor.get_highest_seen();
			if (h != nr_entries_ - 1) {
				array_detail::damage d(run<unsigned>(h + 1, nr_entries_), "missing blocks");
				damage_visitor.visit(d);
			}
		}

		void count_metadata_blocks(block_counter &bc) const {
			block_address_counter vc(bc);
			count_btree_blocks(block_tree_, bc, vc);
		}

	private:

		struct resizer {
			resizer(array<ValueTraits> &a,
				unsigned old_size,
				unsigned new_size,
				unsigned entries_per_block,
				typename ValueTraits::value_type const &v)
				: a_(a),
				  old_dim_(old_size, entries_per_block),
				  new_dim_(new_size, entries_per_block),
				  entries_per_block_(entries_per_block),
				  v_(v) {
			}

			void grow(unsigned new_nr_entries, value_type const &v) {
				if (new_dim_.nr_full_blocks > old_dim_.nr_full_blocks)
					grow_needs_more_blocks();

				else if (old_dim_.nr_entries_in_last_block > 0)
					grow_extend_tail_block(new_dim_.nr_entries_in_last_block);

				else if (new_dim_.nr_entries_in_last_block)
					grow_add_tail_block();

				a_.nr_entries_ = new_nr_entries;
			}

		private:
			void insert_full_ablocks(unsigned begin_index, unsigned end_index) {
				while (begin_index != end_index) {
					wblock b = a_.new_ablock(begin_index);
					b.grow(entries_per_block_, v_);

					begin_index++;
				}
			}

			void grow_add_tail_block() {
				wblock b = a_.new_ablock(new_dim_.nr_full_blocks);
				b.grow(new_dim_.nr_entries_in_last_block, v_);
			}

			void grow_needs_more_blocks() {
				if (old_dim_.nr_entries_in_last_block > 0)
					grow_extend_tail_block(entries_per_block_);

				insert_full_ablocks(old_dim_.nr_total_blocks, new_dim_.nr_full_blocks);

				if (new_dim_.nr_entries_in_last_block > 0)
					grow_add_tail_block();
			}

			void grow_extend_tail_block(unsigned new_nr_entries) {
				uint64_t last_block = a_.nr_entries_ / entries_per_block_;
				wblock b = a_.shadow_ablock(last_block);
				b.grow(new_nr_entries, v_);
			}

			array<ValueTraits> &a_;
			array_detail::array_dim old_dim_;
			array_detail::array_dim new_dim_;
			unsigned entries_per_block_;

			typename ValueTraits::value_type const &v_;
		};

		friend class resizer;

		//--------------------------------

		block_address lookup_block_address(unsigned array_index) const {
			uint64_t key[1] = {array_index};
			boost::optional<uint64_t> addr = block_tree_.lookup(key);
			if (!addr) {
				std::ostringstream str;
				str << "lookup of array block " << array_index << " failed";
				throw runtime_error(str.str());
			}

			return *addr;
		}

		wblock new_ablock(unsigned ablock_index) {
			uint64_t key[1] = {ablock_index};
			write_ref b = tm_.new_block(validator_);
			block_address location = b.get_location();

			wblock wb(b, rc_);
			wb.setup_empty();
			block_tree_.insert(key, location);
			return wblock(b, rc_);
		}

		rblock get_ablock(unsigned ablock_index) const {
			block_address addr = lookup_block_address(ablock_index);
			return rblock(tm_.read_lock(addr, validator_), rc_);
		}

		wblock shadow_ablock(unsigned ablock_index) {
			uint64_t key[1] = {ablock_index};
			block_address addr = lookup_block_address(ablock_index);
			std::pair<write_ref, bool> p = tm_.shadow(addr, validator_);
			wblock wb = wblock(p.first, rc_);

			if (p.second)
				wb.inc_all_entries();

			block_tree_.insert(key, p.first.get_location());

			return wb;
		}

		void dec_ablock_entries(block_address addr) {
			rblock b(tm_.read_lock(addr, validator_), rc_);
			b.dec_all_entries();
		}

		transaction_manager &tm_;
		unsigned entries_per_block_;
		unsigned nr_entries_;
		block_ref_counter block_rc_;
		btree<1, block_traits> block_tree_;
		typename ValueTraits::ref_counter rc_;
		bcache::validator::ptr validator_;
	};
}

//----------------------------------------------------------------

#endif

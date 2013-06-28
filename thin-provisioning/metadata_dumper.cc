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

#include "metadata_dumper.h"
#include "thin-provisioning/mapping_tree.h"

using namespace persistent_data;
using namespace thin_provisioning;

//----------------------------------------------------------------

namespace {
	class mappings_extractor : public mapping_tree::visitor {
	public:
		typedef boost::shared_ptr<mappings_extractor> ptr;
		typedef btree_detail::node_location node_location;
		typedef btree_checker<2, mapping_tree_detail::block_traits> checker;

		mappings_extractor(uint64_t dev_id, emitter::ptr e,
				   space_map::ptr md_sm, space_map::ptr data_sm)
			: counter_(),
			  checker_(counter_, false),
			  dev_id_(dev_id),
			  e_(e),
			  md_sm_(md_sm),
			  data_sm_(data_sm),
			  in_range_(false),
			  found_errors_(false) {
		}

		bool visit_internal(node_location const &loc,
				    btree_detail::node_ref<block_traits> const &n) {

			if (!checker_.visit_internal(loc, n)) {
				found_errors_ = true;
				return false;
			}

			return loc.is_sub_root() ? (loc.path[0] == dev_id_) : true;
		}

		bool visit_internal_leaf(node_location const &loc,
					 btree_detail::node_ref<block_traits> const &n) {
			if (!checker_.visit_internal_leaf(loc, n)) {
				found_errors_ = true;
				return false;
			}

			return true;
		}

		bool visit_leaf(node_location const &loc,
				btree_detail::node_ref<mapping_tree_detail::block_traits> const &n) {
			if (!checker_.visit_leaf(loc, n)) {
				found_errors_ = true;
				return false;
			}

			for (unsigned i = 0; i < n.get_nr_entries(); i++) {
				mapping_tree_detail::block_time bt = n.value_at(i);
				add_mapping(n.key_at(i), bt.block_, bt.time_);
			}

			return true;
		}

		void visit_complete() {
			end_mapping();
		}

		bool corruption() const {
			return !checker_.get_errors()->empty();
		}

	private:
		void start_mapping(uint64_t origin_block, uint64_t dest_block, uint32_t time) {
			origin_start_ = origin_block;
			dest_start_ = dest_block;
			time_ = time;
			len_ = 1;
			in_range_ = true;
		}

		void end_mapping() {
			if (in_range_) {
				if (len_ == 1)
					e_->single_map(origin_start_, dest_start_, time_);
				else
					e_->range_map(origin_start_, dest_start_, time_, len_);

				in_range_ = false;
			}
		}

		void add_mapping(uint64_t origin_block, uint64_t dest_block, uint32_t time) {
			if (!in_range_)
				start_mapping(origin_block, dest_block, time);

			else if (origin_block == origin_start_ + len_ &&
				 dest_block == dest_start_ + len_ &&
				 time == time_)
				len_++;

			else {
				end_mapping();
				start_mapping(origin_block, dest_block, time);
			}
		}

		// Declaration order of counter_ and checker_ is important.
		block_counter counter_;
		checker checker_;
		uint64_t dev_id_;
		emitter::ptr e_;
		space_map::ptr md_sm_;
		space_map::ptr data_sm_;

		bool in_range_;
		uint64_t origin_start_, dest_start_, len_;
		uint32_t time_;
		bool found_errors_;
	};

	class details_extractor : public btree<1, device_tree_detail::device_details_traits>::visitor {
	public:
		typedef typename btree<1, device_tree_detail::device_details_traits>::visitor::node_location node_location;
		typedef boost::shared_ptr<details_extractor> ptr;
		typedef btree_checker<1, device_tree_detail::device_details_traits> checker;

		details_extractor()
			: counter_(),
			  checker_(counter_, false) {
		}

		bool visit_internal(node_location const &loc,
				    btree_detail::node_ref<block_traits> const &n) {
			return checker_.visit_internal(loc, n);
		}

		bool visit_internal_leaf(node_location const &loc,
					 btree_detail::node_ref<block_traits> const &n) {
			return checker_.visit_internal_leaf(loc, n);
		}

		bool visit_leaf(node_location const &loc,
				btree_detail::node_ref<device_tree_detail::device_details_traits> const &n) {
			if (!checker_.visit_leaf(loc, n))
				return false;

			for (unsigned i = 0; i < n.get_nr_entries(); i++)
				devices_.insert(make_pair(n.key_at(i), n.value_at(i)));

			return true;
		}

		map<uint64_t, device_tree_detail::device_details> const &get_devices() const {
			return devices_;
		}

		bool corruption() const {
			return !checker_.get_errors()->empty();
		}

	private:
		// Declaration order of counter_ and checker_ is important.
		block_counter counter_;
		checker checker_;
		map<uint64_t, device_tree_detail::device_details> devices_;
	};
}

//----------------------------------------------------------------

void
thin_provisioning::metadata_dump(metadata::ptr md, emitter::ptr e, bool repair)
{
	boost::optional<uint64_t> md_snap = md->sb_.metadata_snap_ ?
		boost::optional<uint64_t>(md->sb_.metadata_snap_) :
		boost::optional<uint64_t>();

	e->begin_superblock("", md->sb_.time_,
			    md->sb_.trans_id_,
			    md->sb_.data_block_size_,
			    md->data_sm_->get_nr_blocks(),
			    md_snap);

	details_extractor de;
	md->details_->visit_depth_first(de);
	if (de.corruption() && !repair)
		throw runtime_error("corruption in device details tree");

	map<uint64_t, device_tree_detail::device_details> const &devs = de.get_devices();

	map<uint64_t, device_tree_detail::device_details>::const_iterator it, end = devs.end();
	for (it = devs.begin(); it != end; ++it) {
		uint64_t dev_id = it->first;
		device_tree_detail::device_details const &dd = it->second;

		e->begin_device(dev_id,
				dd.mapped_blocks_,
				dd.transaction_id_,
				dd.creation_time_,
				dd.snapshotted_time_);

		mappings_extractor me(dev_id, e, md->metadata_sm_, md->data_sm_);
		md->mappings_->visit_depth_first(me);

		if (me.corruption() && !repair) {
			ostringstream out;
			out << "corruption in mappings for device " << dev_id;
			throw runtime_error(out.str());
		}

		e->end_device();
	}

	e->end_superblock();
}

//----------------------------------------------------------------

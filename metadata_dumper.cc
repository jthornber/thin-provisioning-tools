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

using namespace persistent_data;
using namespace thin_provisioning;

//----------------------------------------------------------------

namespace {
	class mappings_extractor : public btree<2, block_traits>::visitor {
	public:
		typedef boost::shared_ptr<mappings_extractor> ptr;

		mappings_extractor(uint64_t dev_id, emitter::ptr e,
				   space_map::ptr md_sm, space_map::ptr data_sm)
			: dev_id_(dev_id),
			  e_(e),
			  md_sm_(md_sm),
			  data_sm_(data_sm),
			  in_range_(false) {
		}

		bool visit_internal(unsigned level, bool sub_root, boost::optional<uint64_t> key,
				    btree_detail::node_ref<uint64_traits> const &n) {
			return (sub_root && key) ? (*key == dev_id_) : true;
		}

		bool visit_internal_leaf(unsigned level, bool sub_root, boost::optional<uint64_t> key,
					 btree_detail::node_ref<uint64_traits> const &n) {
			return true;
		}

		bool visit_leaf(unsigned level, bool sub_root, boost::optional<uint64_t> maybe_key,
				btree_detail::node_ref<block_traits> const &n) {
			for (unsigned i = 0; i < n.get_nr_entries(); i++) {
				block_time bt = n.value_at(i);
				add_mapping(n.key_at(i), bt.block_, bt.time_);
			}

			return true;
		}


		void visit_complete() {
			end_mapping();
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

		uint64_t dev_id_;
		emitter::ptr e_;
		space_map::ptr md_sm_;
		space_map::ptr data_sm_;

		bool in_range_;
		uint64_t origin_start_, dest_start_, len_;
		uint32_t time_;

	};

	class details_extractor : public btree<1, device_details_traits>::visitor {
	public:
		typedef boost::shared_ptr<details_extractor> ptr;

		details_extractor() {
		}

		bool visit_internal(unsigned level, bool sub_root, boost::optional<uint64_t> key,
				    btree_detail::node_ref<uint64_traits> const &n) {
			return true;
		}

		bool visit_internal_leaf(unsigned level, bool sub_root, boost::optional<uint64_t> key,
					 btree_detail::node_ref<uint64_traits> const &n) {
			return true;
		}

		bool visit_leaf(unsigned level, bool sub_root, boost::optional<uint64_t> maybe_key,
				btree_detail::node_ref<device_details_traits> const &n) {
			for (unsigned i = 0; i < n.get_nr_entries(); i++)
				devices_.insert(make_pair(n.key_at(i), n.value_at(i)));

			return true;
		}

		map<uint64_t, device_details> const &get_devices() const {
			return devices_;
		}

	private:
		map<uint64_t, device_details> devices_;
	};
}

//----------------------------------------------------------------

void
thin_provisioning::metadata_dump(metadata::ptr md, emitter::ptr e)
{
	e->begin_superblock("", md->sb_.time_, md->sb_.trans_id_, md->sb_.data_block_size_);

	details_extractor::ptr de(new details_extractor);

	md->details_->visit(de);
	map<uint64_t, device_details> const &devs = de->get_devices();

	map<uint64_t, device_details>::const_iterator it, end = devs.end();
	for (it = devs.begin(); it != end; ++it) {
		uint64_t dev_id = it->first;
		device_details const &dd = it->second;

		e->begin_device(dev_id,
				dd.mapped_blocks_,
				dd.transaction_id_,
				dd.creation_time_,
				dd.snapshotted_time_);

		mappings_extractor::ptr me(new mappings_extractor(dev_id, e, md->metadata_sm_, md->data_sm_));
		md->mappings_->visit(me);

		e->end_device();
	}

	e->end_superblock();
}

//----------------------------------------------------------------

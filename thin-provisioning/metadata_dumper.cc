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

#include "thin-provisioning/emitter.h"
#include "thin-provisioning/metadata_dumper.h"
#include "thin-provisioning/mapping_tree.h"

using namespace persistent_data;
using namespace thin_provisioning;

//----------------------------------------------------------------

namespace {
	void raise_metadata_damage() {
		throw std::runtime_error("metadata contains errors (run thin_check for details).\n"
					 "perhaps you wanted to run with --repair");
	}

	//--------------------------------

	struct ignore_details_damage : public device_tree_detail::damage_visitor {
		void visit(device_tree_detail::missing_devices const &d) {
		}
	};

	struct fatal_details_damage : public device_tree_detail::damage_visitor {
		void visit(device_tree_detail::missing_devices const &d) {
			raise_metadata_damage();
		}
	};

	device_tree_detail::damage_visitor::ptr details_damage_policy(bool repair) {
		typedef device_tree_detail::damage_visitor::ptr dvp;

		if (repair)
			return dvp(new ignore_details_damage());
		else
			return dvp(new fatal_details_damage());
	}

	//--------------------------------

	struct ignore_mapping_damage : public mapping_tree_detail::damage_visitor {
		void visit(mapping_tree_detail::missing_devices const &d) {
		}

		void visit(mapping_tree_detail::missing_mappings const &d) {
		}
	};

	struct fatal_mapping_damage : public mapping_tree_detail::damage_visitor {
		void visit(mapping_tree_detail::missing_devices const &d) {
			raise_metadata_damage();
		}

		void visit(mapping_tree_detail::missing_mappings const &d) {
			raise_metadata_damage();
		}
	};

	mapping_tree_detail::damage_visitor::ptr mapping_damage_policy(bool repair) {
		typedef mapping_tree_detail::damage_visitor::ptr mvp;

		if (repair)
			return mvp(new ignore_mapping_damage());
		else
			return mvp(new fatal_mapping_damage());
	}

	//--------------------------------

	typedef map<block_address, device_tree_detail::device_details> dd_map;

	class details_extractor : public device_tree_detail::device_visitor {
	public:
		void visit(block_address dev_id, device_tree_detail::device_details const &dd) {
			dd_.insert(make_pair(dev_id, dd));
		}

		dd_map const &get_details() const {
			return dd_;
		}

	private:
		dd_map dd_;
	};

	class mapping_emitter : public mapping_tree_detail::mapping_visitor {
	public:
		mapping_emitter(emitter::ptr e)
			: e_(e),
			  in_range_(false) {
		}

		~mapping_emitter() {
			end_mapping();
		}

		typedef mapping_tree_detail::block_time block_time;
		void visit(btree_path const &path, block_time const &bt) {
			add_mapping(path[0], bt);
		}

	private:
		void start_mapping(uint64_t origin_block, block_time const &bt) {
			origin_start_ = origin_block;
			dest_start_ = bt.block_;
			time_ = bt.time_;
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

		void add_mapping(uint64_t origin_block, block_time const &bt) {
			if (!in_range_)
				start_mapping(origin_block, bt);

			else if (origin_block == origin_start_ + len_ &&
				 bt.block_ == dest_start_ + len_ &&
				 time_ == bt.time_)
				len_++;

			else {
				end_mapping();
				start_mapping(origin_block, bt);
			}
		}

		emitter::ptr e_;
		block_address origin_start_;
		block_address dest_start_;
		uint32_t time_;
		block_address len_;
		bool in_range_;
	};

	class mapping_tree_emitter : public mapping_tree_detail::device_visitor {
	public:
		mapping_tree_emitter(metadata::ptr md,
				     emitter::ptr e,
				     dd_map const &dd,
				     bool repair,
				     mapping_tree_detail::damage_visitor::ptr damage_policy)
			: md_(md),
			  e_(e),
			  dd_(dd),
			  repair_(repair),
			  damage_policy_(damage_policy) {
		}

		void visit(btree_path const &path, block_address tree_root) {
			block_address dev_id = path[0];

			dd_map::const_iterator it = dd_.find(path[0]);
			if (it != dd_.end()) {
				device_tree_detail::device_details const &d = it->second;
				e_->begin_device(dev_id,
						 d.mapped_blocks_,
						 d.transaction_id_,
						 d.creation_time_,
						 d.snapshotted_time_);

				emit_mappings(tree_root);

				e_->end_device();

			} else if (!repair_) {
				ostringstream msg;
				msg << "mappings present for device " << dev_id
				    << ", but it isn't present in device tree";
				throw runtime_error(msg.str());
			}
		}

	private:
		void emit_mappings(block_address subtree_root) {
			mapping_emitter me(e_);
			single_mapping_tree tree(md_->tm_, subtree_root,
						 mapping_tree_detail::block_time_ref_counter(md_->data_sm_));
			walk_mapping_tree(tree, static_cast<mapping_tree_detail::mapping_visitor &>(me), *damage_policy_);
		}

		metadata::ptr md_;
		emitter::ptr e_;
		dd_map const &dd_;
		bool repair_;
		mapping_tree_detail::damage_visitor::ptr damage_policy_;
	};
}

//----------------------------------------------------------------

void
thin_provisioning::metadata_dump(metadata::ptr md, emitter::ptr e, bool repair)
{
	details_extractor de;
	device_tree_detail::damage_visitor::ptr dd_policy(details_damage_policy(repair));
	walk_device_tree(*md->details_, de, *dd_policy);

	e->begin_superblock("", md->sb_.time_,
			    md->sb_.trans_id_,
			    md->sb_.data_block_size_,
			    md->data_sm_->get_nr_blocks(),
			    optional<block_address>());

	{
		mapping_tree_detail::damage_visitor::ptr md_policy(mapping_damage_policy(repair));
		mapping_tree_emitter mte(md, e, de.get_details(), repair, mapping_damage_policy(repair));
		walk_mapping_tree(*md->mappings_top_level_, mte, *md_policy);
	}

	e->end_superblock();
}

//----------------------------------------------------------------

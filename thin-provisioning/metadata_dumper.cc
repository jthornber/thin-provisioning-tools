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

#include <map>
#include <vector>

using namespace persistent_data;
using namespace thin_provisioning;

//----------------------------------------------------------------

// We only need to examine the mapping tree, and device details tree.
// The space maps can be inferred.

// Repair process:
// - We only trigger the repair process if there's damage when walking from
//   the roots given in the superblock.
// - If there is damage, then we try and find the most recent roots with the
//   least corruption.  We're seeing cases where just the superblock has been
//   trashed so finding the best roots is essential, and sadly non trivial.

// Finding roots:
// This is about classifying and summarising btree nodes.  The use of a btree
// node may not be obvious when inspecting it in isolation.  But more information
// may be gleaned by examining child and sibling nodes.
// 
// So the process is:
// - scan every metadata block, summarising it's potential uses.
// - repeatedly iterate those summaries until we can glean no more useful information.
// - sort candidate roots, choose best

// Summary information:
// - btree; mapping top level, mapping bottom level, device tree (more than one possible)
// - node type; internal or leaf
// - age; for mapping trees we can infer a minimum age from the block/time
//   values.  In addition two similar leaf nodes can be compared by looking
//   at the block/time for _specific_ blocks.  This means we can define an ordering
//   on the ages, but not equality.
// - Device details can be aged based on the last_snapshot_time field.

// Iteration of summary info:
// - constraints propagate both up and down the trees.  eg, node 'a' may
//   be ambiguous (all internal nodes are ambigous).  If we find that all it's
//   children are device details trees, then we infer that this is too and lose
//   the ambiguity.  Now if it has a sibling we can infer on this too.
// - Some characteristics only propagate upwards.  eg, age.  So we need two monoids
//   for summary info (up and down).

namespace {
	using namespace std;
	using namespace boost;
	using namespace persistent_data::btree_detail;
	using namespace thin_provisioning::device_tree_detail;

	enum btree_type_bit {
		TOP_LEVEL,
		BOTTOM_LEVEL,
		DEVICE_DETAILS
	};

	struct node_info {
		node_info()
			: types(0),
			  b(0),
			  values(0),
			  orphan(true),
			  is_leaf(true),
			  key_low(0),
			  key_high(0),
			  age(0) {
		}

		void add_type(btree_type_bit b) {
			types = types | (1 << b);
		}

		void clear_type(btree_type_bit b) {
			types = types & ~(1 << b);
		}

		bool has_type(btree_type_bit b) const {
			return types & (1 << b);
		}

		// Indicate corruption by having no fields set
		unsigned types;

		// common
		block_address b;
		unsigned values;
		bool orphan;
		bool is_leaf;
		uint64_t key_low;
		uint64_t key_high;
		set<uint32_t> devices;
		uint32_t age;
	};

	using info_map = map<block_address, node_info>;

	bool is_btree_node(block_manager<> &bm, block_address b) {
		auto v = create_btree_node_validator();
		auto rr = bm.read_lock(b);

		return v->check_raw(rr.data());
	}

	uint32_t get_dd_age(device_details const &dd) {
		return max(dd.creation_time_, dd.snapshotted_time_);
	}

	void scan_initial_infos(block_manager<> &bm, info_map &result) {
		for (block_address b = 0; b < bm.get_nr_blocks(); b++) {
			if (!is_btree_node(bm, b))
				continue;

			node_info info;
			info.b = b;

			auto rr = bm.read_lock(b);
			auto hdr = reinterpret_cast<node_header const *>(rr.data());

			auto flags = to_cpu<uint32_t>(hdr->flags);
			if (flags & INTERNAL_NODE) {
				info.is_leaf = false;
				info.add_type(TOP_LEVEL);
				info.add_type(BOTTOM_LEVEL);
				info.add_type(DEVICE_DETAILS);
			} else {
				info.is_leaf = true;
				auto vsize = to_cpu<uint32_t>(hdr->value_size); 
				info.values = to_cpu<uint32_t>(hdr->nr_entries);

				if (vsize == sizeof(device_details_traits::disk_type)) {
					info.add_type(DEVICE_DETAILS);

					auto n = to_node<device_details_traits>(rr);
					if (n.get_nr_entries()) {
						info.key_low = n.key_at(0);
						info.key_high = n.key_at(n.get_nr_entries() - 1);
					}

					for (unsigned i = 0; i < n.get_nr_entries(); i++)
						info.age = max(info.age, get_dd_age(n.value_at(i)));

				} else if (vsize == sizeof(uint64_t)) {
					info.add_type(BOTTOM_LEVEL);

					// This can only be a top level leaf if all the values are
					// blocks on the metadata device.
					auto is_top_level = true;
					auto n = to_node<block_traits>(rr);

					if (n.get_nr_entries()) {
						info.key_low = n.key_at(0);
						info.key_high = n.key_at(n.get_nr_entries() - 1);
					}

					for (unsigned i = 0; i < n.get_nr_entries(); i++) {
						if (n.value_at(i) >= bm.get_nr_blocks()) {
							is_top_level = false;
							break;
						}
					}

					if (is_top_level)
						info.add_type(TOP_LEVEL);
				} else
					continue;
			}

			result.insert(make_pair(b, info));
		}
	}

	bool merge_types(node_info &parent, node_info const &child, btree_type_bit b) {
		if (parent.has_type(b) && !child.has_type(b)) {
			parent.clear_type(b);
			return true;
		}

		return false;
	}

	// return true if something changed
	bool merge_from_below(node_info &parent, node_info const &child) {
		bool changed = false;

		changed = merge_types(parent, child, TOP_LEVEL) ||
			merge_types(parent, child, BOTTOM_LEVEL) ||
			merge_types(parent, child, DEVICE_DETAILS);

		return changed;
	}

	void fail(node_info &n) {
		n.types = 0;
	}

	bool failed(node_info const &n) {
		return n.types == 0;
	}

	bool iterate_infos_(block_manager<> &bm, info_map &infos) {
		bool changed = false;

		for (auto &p : infos) {
			auto &parent = p.second;

			if (parent.is_leaf)
				continue;

			// values refer to blocks, so we should have infos for them.
			auto rr = bm.read_lock(p.first);
			auto n = to_node<block_traits>(rr);
			uint64_t key_low = 0;
			unsigned values = 0;

			for (unsigned i = 0; i < n.get_nr_entries(); i++) {
				auto it = infos.find(n.value_at(i));

				if (it == infos.end()) {
					fail(parent);
					break;
				}

				auto &child = it->second;

				// we use the keys to help decide if this is a valid child
				if (child.key_low <= key_low) {
					fail(parent);
					break;

				} else
					key_low = child.key_high;


				changed = merge_from_below(parent, child) || changed;

				if (parent.has_type(DEVICE_DETAILS) && child.age > parent.age) {
					changed = true;
					parent.age = child.age;
				}

				values += child.values;
			}

			// We don't clear the orphan flags until we know the parent is good
			if (!failed(parent)) {
				parent.values = values;

				for (unsigned i = 0; i < n.get_nr_entries(); i++) {
					auto it = infos.find(n.value_at(i));

					if (it == infos.end())
						throw runtime_error("no child info, but it was there a moment ago");

					auto &child = it->second;
					child.orphan = false;
				}
			}
		}

		return changed;
	}

	void iterate_infos(block_manager<> &bm, info_map &infos) {
		while (iterate_infos_(bm, infos))
			;
	}

	bool trees_are_compatible(node_info const &mapping, node_info const &devices) {
		for (auto thin_id : mapping.devices)
			if (devices.devices.find(thin_id) == devices.devices.end())
				return false;

		return true;
	}

	bool cmp_mapping_info(node_info const &lhs, node_info const &rhs) {
		return lhs.age > rhs.age;
	}

	bool has_type(node_info const &i, unsigned bit) {
		return i.types & (1 << bit);
	}

	vector<node_info>
	extract_mapping_candidates(info_map const &infos) {
		vector<node_info> results;

		for (auto const &p : infos)
			if (p.second.orphan && has_type(p.second, TOP_LEVEL))
				results.push_back(p.second);

		//sort(results.begin(), results.end(), cmp_mapping_info);

		return results;
	}

	bool cmp_device_info(node_info const &lhs, node_info const &rhs) {
		// FIXME: finish
		return false;
		//return lhs.dd_age > rhs.dd_age;
	}

	vector<node_info>
	extract_device_candidates(info_map const &infos) {
		vector<node_info> results;

		for (auto const &p : infos)
			if (p.second.orphan && has_type(p.second, DEVICE_DETAILS))
				results.push_back(p.second);

		sort(results.begin(), results.end(), cmp_device_info);

		return results;
	}

	// Returns <mapping root>, <dev details root>
	//pair<block_address, block_address>
	void
	find_best_roots(block_manager<> &bm) {
		info_map infos;

		scan_initial_infos(bm, infos);
		iterate_infos(bm, infos);

		// These will be sorted into best first order
		vector<node_info> mapping_candidates = extract_mapping_candidates(infos);
		vector<node_info> device_candidates = extract_device_candidates(infos);

		cerr << "mapping candidates (" << mapping_candidates.size() << "):\n";
		for (auto const &i : mapping_candidates)
			cerr << i.b << ", tree size = " << i.values << ", age = " << i.age << "\n";

		cerr << "\ndevice candidates (" << device_candidates.size() << "):\n";
		for (auto const &i : device_candidates)
			cerr << i.b << ", tree size = " << i.values << ", age = " << i.age << "\n";

#if 0
		// Choose the best mapping tree, and then the best device tree
		// that is compatible.
		for (auto &m : mapping_candidates)
			for (auto &d : device_candidates)
				if (trees_are_compatible(m, d))
					return make_pair(m.b, d.b);
#endif

//		throw runtime_error("no compatible mapping/device trees");
	}
}

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
		details_extractor(dump_options const &opts)
		: opts_(opts) {
		}

		void visit(block_address dev_id, device_tree_detail::device_details const &dd) {
			if (opts_.selected_dev(dev_id))
				dd_.insert(make_pair(dev_id, dd));
		}

		dd_map const &get_details() const {
			return dd_;
		}

	private:
		dump_options const &opts_;
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
		mapping_tree_emitter(dump_options const &opts,
				     metadata::ptr md,
				     emitter::ptr e,
				     dd_map const &dd,
				     mapping_tree_detail::damage_visitor::ptr damage_policy)
			: opts_(opts),
			  md_(md),
			  e_(e),
			  dd_(dd),
			  damage_policy_(damage_policy) {
		}

		void visit(btree_path const &path, block_address tree_root) {
			block_address dev_id = path[0];

			if (!opts_.selected_dev(dev_id))
				return;

			dd_map::const_iterator it = dd_.find(path[0]);
			if (it != dd_.end()) {
				device_tree_detail::device_details const &d = it->second;
				e_->begin_device(dev_id,
						 d.mapped_blocks_,
						 d.transaction_id_,
						 d.creation_time_,
						 d.snapshotted_time_);

				try {
					if (!opts_.skip_mappings_)
						emit_mappings(dev_id, tree_root);
				} catch (std::exception &e) {
					cerr << e.what();
					e_->end_device();
					throw;
				}
				e_->end_device();

			} else if (!opts_.repair_) {
				ostringstream msg;
				msg << "mappings present for device " << dev_id
				    << ", but it isn't present in device tree";
				throw runtime_error(msg.str());
			}
		}

	private:
		void emit_mappings(uint64_t dev_id, block_address subtree_root) {
			mapping_emitter me(e_);
			single_mapping_tree tree(*md_->tm_, subtree_root,
						 mapping_tree_detail::block_time_ref_counter(md_->data_sm_));
			walk_mapping_tree(tree, dev_id, static_cast<mapping_tree_detail::mapping_visitor &>(me), *damage_policy_);
		}

		dump_options const &opts_;
		metadata::ptr md_;
		emitter::ptr e_;
		dd_map const &dd_;
		mapping_tree_detail::damage_visitor::ptr damage_policy_;
	};

	block_address get_nr_blocks(metadata::ptr md) {
		if (md->data_sm_)
			return md->data_sm_->get_nr_blocks();

		else if (md->sb_.blocknr_ == superblock_detail::SUPERBLOCK_LOCATION)
			// grab from the root structure of the space map
			return get_nr_blocks_in_data_sm(*md->tm_, &md->sb_.data_space_map_root_);

		else
			// metadata snap, we really don't know
			return 0ull;
	}
}

//----------------------------------------------------------------

void
thin_provisioning::metadata_dump(metadata::ptr md, emitter::ptr e, dump_options const &opts)
{
	find_best_roots(*md->tm_->get_bm());

	details_extractor de(opts);
	device_tree_detail::damage_visitor::ptr dd_policy(details_damage_policy(opts.repair_));
	walk_device_tree(*md->details_, de, *dd_policy);

	e->begin_superblock("", md->sb_.time_,
			    md->sb_.trans_id_,
			    md->sb_.flags_,
			    md->sb_.version_,
			    md->sb_.data_block_size_,
			    get_nr_blocks(md),
			    boost::optional<block_address>());

	{
		mapping_tree_detail::damage_visitor::ptr md_policy(mapping_damage_policy(opts.repair_));
		mapping_tree_emitter mte(opts, md, e, de.get_details(), mapping_damage_policy(opts.repair_));
		walk_mapping_tree(*md->mappings_top_level_, mte, *md_policy);
	}

	e->end_superblock();
}

//----------------------------------------------------------------

void
thin_provisioning::metadata_dump_subtree(metadata::ptr md, emitter::ptr e, bool repair, uint64_t subtree_root) {
	mapping_emitter me(e);
	single_mapping_tree tree(*md->tm_, subtree_root,
				 mapping_tree_detail::block_time_ref_counter(md->data_sm_));
	// FIXME: pass the current device id instead of zero
	walk_mapping_tree(tree, 0, static_cast<mapping_tree_detail::mapping_visitor &>(me),
			  *mapping_damage_policy(repair));
}

//----------------------------------------------------------------

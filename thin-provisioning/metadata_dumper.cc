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

#include "persistent-data/data-structures/simple_traits.h"
#include "persistent-data/file_utils.h"
#include "persistent-data/space-maps/noop.h"
#include "thin-provisioning/emitter.h"
#include "thin-provisioning/mapping_tree.h"
#include "thin-provisioning/metadata_dumper.h"

#include <algorithm>
#include <map>
#include <vector>

using namespace boost;
using namespace persistent_data;
using namespace thin_provisioning;

#define SHOW_WORKING 0

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

	struct d_thin_id_extractor : public device_tree_detail::device_visitor {
		void visit(block_address dev_id, device_tree_detail::device_details const &dd) {
			dd_.insert(dev_id);
		}

		set<uint32_t> dd_;
	};

	// See comment on get_map_ids
	optional<set<uint32_t> >
	get_dev_ids(transaction_manager &tm, block_address root) {
		d_thin_id_extractor de;
		fatal_details_damage dv;
		auto tree = device_tree(tm, root, device_tree_detail::device_details_traits::ref_counter());

		try {
			walk_device_tree(tree, de, dv);
		} catch (...) {
			return optional<set<uint32_t>>();
		}

		return de.dd_;
	}

	struct m_thin_id_extractor : public mapping_tree_detail::device_visitor {
		void visit(btree_path const &path, block_address dtree_root) {
			dd_.insert(path[0]);
		}

		set<uint32_t> dd_;
	};

	// The walk will do more sanity checks than we did when scanning the metadata, so
	// it's possible that it will fail and throw a metadata damage exception.
	optional<set<uint32_t> >
	get_map_ids(transaction_manager &tm, block_address root) {
		m_thin_id_extractor me;
		fatal_mapping_damage mv;
		auto tree = dev_tree(tm, root, mapping_tree_detail::mtree_traits::ref_counter(tm));

		try {
			walk_mapping_tree(tree, me, mv);
		} catch (...) {
			return optional<set<uint32_t>>();
		}

		return me.dd_;
	}
}

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

	enum btree_type {
		TOP_LEVEL,
		BOTTOM_LEVEL,
		DEVICE_DETAILS
	};

	struct node_info {
		node_info()
			: valid(true),
			  type(TOP_LEVEL),
			  b(0),
			  values(0),
			  key_low(0),
			  key_high(0),
			  age(0),
			  nr_mappings(0) {
		}

		bool valid;
		btree_type type;

		block_address b;
		unsigned values;
		uint64_t key_low;
		uint64_t key_high;
		//set<uint32_t> devices;
		uint32_t age;
		map<uint32_t, uint32_t> time_counts;
		unsigned nr_mappings;
	};

#if SHOW_WORKING
	ostream &operator <<(ostream &out, node_info const &n) {
		out << "b=" << n.b << ", valid=" << n.valid << ", type=" << n.type << ", values=" << n.values;
		out << ", nr_mapped=" << n.nr_mappings;
		for (auto const &p : n.time_counts)
			out << ", t" << p.first << "=" << p.second;
		return out;
	}
#endif

	bool cmp_time_counts(pair<node_info, node_info> const &lhs_pair,
                             pair<node_info, node_info> const &rhs_pair) {
	        auto const &lhs = lhs_pair.first.time_counts;
	        auto const &rhs = rhs_pair.first.time_counts;

	        for (auto lhs_it = lhs.crbegin(); lhs_it != lhs.crend(); lhs_it++) {
		        for (auto rhs_it = rhs.crbegin(); rhs_it != rhs.crend(); rhs_it++) {
			        if (lhs_it->first > rhs_it->first)
				        return true;

			        else if (rhs_it->first > lhs_it->first)
				        return false;

				else if (lhs_it->second > rhs_it->second)
					return true;

				else if (rhs_it->second > lhs_it->second)
					return false;
		        }
	        }

	        return true;
        }

	class gatherer {
	public:
                gatherer(block_manager<> &bm)
                        : bm_(bm),
                          referenced_(bm.get_nr_blocks(), false),
                          examined_(bm.get_nr_blocks(), false) {
                }

                struct roots {
                        block_address mapping_root;
                        block_address detail_root;
                        uint32_t time;
                };

                optional<roots>

		find_best_roots(transaction_manager &tm) {
			vector<node_info> mapping_roots;
			vector<node_info> device_roots;

			auto nr_blocks = bm_.get_nr_blocks();
			for (block_address b = 0; b < nr_blocks; b++)
				get_info(b);

			for (block_address b = 0; b < nr_blocks; b++) {
				if (referenced(b))
					continue;

				auto info = get_info(b);

                                if (info.valid) {
                                        if (info.type == TOP_LEVEL) {
                                                mapping_roots.push_back(info);
                                        }

                                        else if (info.type == DEVICE_DETAILS) {
                                                device_roots.push_back(info);
                                        }
                                }
                        }

#if SHOW_WORKING
			cerr << "mapping candidates (" << mapping_roots.size() << "):\n";
			for (auto const &i : mapping_roots)
				cerr << i << "\n";

			cerr << "\ndevice candidates (" << device_roots.size() << "):\n";
			for (auto const &i : device_roots)
				cerr << i << "\n";
#endif

			auto pairs = find_compatible_roots(tm, device_roots, mapping_roots);

#if SHOW_WORKING
			for (auto const &p : pairs)
				cerr << "(" << p.first << ", " << p.second << ")\n";
#endif

                        if (pairs.size())
                                return mk_roots(pairs[0]);
                        else
                                return optional<roots>();
                }

	private:
                uint32_t get_time(block_address b) const {
                        auto i = lookup_info(b);
                        return i ? i->age : 0;
                }

                roots mk_roots(pair<block_address, block_address> const &p) {
                        roots r;

                        r.mapping_root = p.second;
                        r.detail_root = p.first;
                        r.time = max<block_address>(get_time(p.first), get_time(p.second));

                        return r;
                }

		bool set_eq(set<uint32_t> const &lhs, set<uint32_t> const &rhs) {
			for (auto v : lhs)
				if (!rhs.count(v))
					return false;

			return true;
		}

		vector<pair<block_address, block_address> >
		find_compatible_roots(transaction_manager &tm,
                                      vector<node_info> const &device_roots,
                                      vector<node_info> const &mapping_roots) {
			vector<pair<node_info, node_info>> pairs;
			set<block_address> d_roots;
			set<block_address> m_roots;

			// construct pairs that have the same number of entries
			for (auto const &di : device_roots)
				for (auto const &mi : mapping_roots)
					if (di.values == mi.values && di.nr_mappings == mi.nr_mappings) {
						pairs.push_back(make_pair(di, mi));
						d_roots.insert(di.b);
						m_roots.insert(mi.b);
					}

			sort(pairs.begin(), pairs.end(), cmp_time_counts);

			map<block_address, set<uint32_t>> ds;
			for (auto b : d_roots) {
				auto maybe_ids = get_dev_ids(tm, b);
				if (maybe_ids)
					ds.insert(make_pair(b, *maybe_ids));
			}

			map<block_address, set<uint32_t>> ms;
			for (auto b : m_roots) {
				auto maybe_ids = get_map_ids(tm, b);
				if (maybe_ids)
					ms.insert(make_pair(b, *maybe_ids));
			}

			// now we check that the thin_ids are identical
			vector<pair<block_address, block_address>> filtered;
			for (auto const &p : pairs) {
				auto lhs = ds.find(p.first.b);
				if (lhs == ds.end())
					continue;

				auto rhs = ms.find(p.second.b);
				if (rhs == ms.end())
					continue;

				filtered.push_back(make_pair(p.first.b, p.second.b));
			}


			return filtered;
		}

		void mark_referenced(block_address b) {
			referenced_[b] = true;
		}

		bool referenced(block_address b) const {
			return referenced_[b];
		}

		bool is_btree_node(block_address b) {
			auto v = create_btree_node_validator();
			auto rr = bm_.read_lock(b);

			return v->check_raw(rr.data());
		}

		// The bottom layer has the block time encoded in it, with the time
		// in the bottom 24 bits.  This means every block/time apart from block 0
		// will result in a value that's outside the range of the metadata device.
		bool is_top_level(node_ref<uint64_traits> &n) {
			auto nr_metadata_blocks = bm_.get_nr_blocks();

			for (unsigned i = 0; i < n.get_nr_entries(); i++)
				if (n.value_at(i) >= nr_metadata_blocks)
					return false;

			return true;
		}

		uint32_t get_dd_age(device_details const &dd) {
			return max(dd.creation_time_, dd.snapshotted_time_);
		}

		void fail(node_info &n, const char *reason) {
			// cerr << n.b << " failed: " << reason << "\n";
			n.valid = false;
		}

		bool failed(node_info const &n) {
			return !n.valid;
		}

		void inc_time_count(map<uint32_t, uint32_t> &counts, uint32_t time) {
			auto it = counts.find(time);
			if (it == counts.end()) {
				counts.insert(make_pair(time, 1));
			} else
				it->second++;
		}

		void merge_time_counts(map<uint32_t, uint32_t> &lhs, map<uint32_t, uint32_t> const &rhs) {
			for (auto const &p : rhs) {
				auto it = lhs.find(p.first);
				if (it == lhs.end())
					lhs.insert(p);
				else
					it->second += p.second;
			}
		}

		node_info get_internal_info(block_manager<>::read_ref &rr) {
			node_info info;
			info.b = rr.get_location();
			 
			// values refer to blocks, so we should have infos for them.
			auto n = to_node<block_traits>(rr);
			uint64_t key_low = 0;
			unsigned values = 0;

			for (unsigned i = 0; i < n.get_nr_entries(); i++) {
				auto child = get_info(n.value_at(i));
				if (failed(child)) {
					fail(info, "child failed");
					break;
				}

				if (!i)
					info.type = child.type;

				else if (info.type != child.type) {
					fail(info, "mismatch types");
					break;
				}

				// we use the keys to help decide if this is a valid child
				if (key_low && child.key_low <= key_low) {
					fail(info, "bad keys");
					break;

				} else
					key_low = child.key_high;

				values += child.values;
				merge_time_counts(info.time_counts, child.time_counts);
				info.age = max(info.age, child.age);
				info.nr_mappings += child.nr_mappings;
			}

			// We don't clear the orphan flags until we know the parent is good
			if (!failed(info)) {
				info.values = values;

				for (unsigned i = 0; i < n.get_nr_entries(); i++)
					mark_referenced(n.value_at(i));
			}

			return info;
		}

		node_info get_leaf_info(block_manager<>::read_ref &rr, node_header const &hdr) {
			node_info info;
			info.b = rr.get_location();

			auto vsize = to_cpu<uint32_t>(hdr.value_size); 
			info.values = to_cpu<uint32_t>(hdr.nr_entries);

			if (vsize == sizeof(device_details_traits::disk_type)) {
				auto n = to_node<device_details_traits>(rr);
				info.type = DEVICE_DETAILS;

				if (n.get_nr_entries()) {
					info.key_low = n.key_at(0);
					info.key_high = n.key_at(n.get_nr_entries() - 1);
				}

				for (unsigned i = 0; i < n.get_nr_entries(); i++) {
					info.age = max(info.age, get_dd_age(n.value_at(i)));
					info.nr_mappings += n.value_at(i).mapped_blocks_;
				}

			} else if (vsize == sizeof(uint64_t)) {
				auto n = to_node<uint64_traits>(rr);

				if (n.get_nr_entries()) {
					info.key_low = n.key_at(0);
					info.key_high = n.key_at(n.get_nr_entries() - 1);
				}

				if (is_top_level(n)) {
					info.type = TOP_LEVEL;

					for (unsigned i = 0; i < n.get_nr_entries(); i++) {
						node_info child = get_info(n.value_at(i));
						if (!child.valid || (child.type != BOTTOM_LEVEL)) {
							fail(info, "child not bottom level");
							return info;
						}

						info.age = max(info.age, child.age);
						merge_time_counts(info.time_counts, child.time_counts);
						info.nr_mappings += child.nr_mappings;
					}

					for (unsigned i = 0; i < n.get_nr_entries(); i++)
						mark_referenced(n.value_at(i));

				} else {
					auto n = to_node<mapping_tree_detail::block_traits>(rr);
					info.type = BOTTOM_LEVEL;

					for (unsigned i = 0; i < n.get_nr_entries(); i++) {
						auto bt = n.value_at(i);
						inc_time_count(info.time_counts, bt.time_);
						info.age = max(info.age, bt.time_);
					}

					info.nr_mappings = n.get_nr_entries();
				}
			}

			return info;
		}

		node_info get_info_(block_address b) {
			if (!is_btree_node(b)) {
				node_info info;
				info.b = b;
				fail(info, "not btree node");
				return info;
			}

			auto rr = bm_.read_lock(b);
			auto hdr = reinterpret_cast<node_header const *>(rr.data());

			auto flags = to_cpu<uint32_t>(hdr->flags);
			if (flags & INTERNAL_NODE)
				return get_internal_info(rr);
			else
				return get_leaf_info(rr, *hdr);
		}

		node_info get_info(block_address b) {
			if (examined_[b]) {
				auto it = infos_.find(b);
				if (it == infos_.end()) {
					node_info info;
					info.b = b;
					fail(info, "unknown");
					return info;
				}

				return it->second;
			} else {
				node_info info = get_info_(b);
				examined_[b] = true;
				if (!failed(info))
					infos_.insert(make_pair(b, info));

				return info;
			}
		}

                optional<node_info> lookup_info(block_address b) const {
                        auto it = infos_.find(b);
                        if (it == infos_.end())
                                return optional<node_info>();

                        return optional<node_info>(it->second);
                }


		block_manager<> &bm_;
		vector<bool> referenced_;
		vector<bool> examined_;
		map<block_address, node_info> infos_;
	};
}

//----------------------------------------------------------------

namespace {
	class mapping_emit_visitor : public mapping_tree_detail::mapping_visitor {
	public:
		mapping_emit_visitor(emitter::ptr e)
			: e_(e),
			  in_range_(false) {
		}

		~mapping_emit_visitor() {
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

	class mapping_tree_emit_visitor : public mapping_tree_detail::device_visitor {
	public:
		mapping_tree_emit_visitor(dump_options const &opts,
					  transaction_manager &tm,
					  emitter::ptr e,
					  dd_map const &dd,
					  mapping_tree_detail::damage_visitor::ptr damage_policy)
			: opts_(opts),
			  tm_(tm),
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

			} else {
				ostringstream msg;
				msg << "mappings present for device " << dev_id
				    << ", but it isn't present in device tree";
				throw runtime_error(msg.str());
			}
		}

	private:
		void emit_mappings(uint64_t dev_id, block_address subtree_root) {
			mapping_emit_visitor me(e_);

			// Since we're not mutating the btrees we don't need a real space map
			noop_map::ptr sm(new noop_map);
			single_mapping_tree tree(tm_, subtree_root, mapping_tree_detail::block_time_ref_counter(sm));
			walk_mapping_tree(tree, dev_id, static_cast<mapping_tree_detail::mapping_visitor &>(me), *damage_policy_);
		}

		dump_options const &opts_;
		transaction_manager &tm_;
		emitter::ptr e_;
		dd_map const &dd_;
		mapping_tree_detail::damage_visitor::ptr damage_policy_;
	};

	block_address get_nr_blocks(metadata &md) {
		if (md.data_sm_)
			return md.data_sm_->get_nr_blocks();

		else if (md.sb_.blocknr_ == superblock_detail::SUPERBLOCK_LOCATION)
			// grab from the root structure of the space map
			return get_nr_blocks_in_data_sm(*md.tm_, &md.sb_.data_space_map_root_);

		else
			// metadata snap, we really don't know
			return 0ull;
	}


        void
        emit_trees_(block_manager<>::ptr bm, superblock_detail::superblock const &sb,
                    emitter::ptr e, override_options const &ropts)
        {
                metadata md(bm, sb);
                dump_options opts;
                details_extractor de(opts);
                device_tree_detail::damage_visitor::ptr dd_policy(details_damage_policy(true));
                walk_device_tree(*md.details_, de, *dd_policy);

                e->begin_superblock("", sb.time_,
                                    sb.trans_id_,
                                    sb.flags_,
                                    sb.version_,
                                    sb.data_block_size_,
                                    get_nr_blocks(md),
                                    boost::optional<block_address>());

                {
                        mapping_tree_detail::damage_visitor::ptr md_policy(mapping_damage_policy(true));
                        mapping_tree_emit_visitor mte(opts, *md.tm_, e, de.get_details(), mapping_damage_policy(true));
                        walk_mapping_tree(*md.mappings_top_level_, mte, *md_policy);
                }

                e->end_superblock();
        }

        void
        find_better_roots_(block_manager<>::ptr bm, superblock_detail::superblock &sb)
        {
                // We assume the superblock is wrong, and find the best roots
                // for ourselves.  We've had a few cases where people have
                // activated a pool on multiple hosts at once, which results in
                // the superblock being over written.
                gatherer g(*bm);
                auto tm = open_tm(bm, superblock_detail::SUPERBLOCK_LOCATION);
                auto p = g.find_best_roots(*tm);

                if (p) {
                        sb.metadata_snap_ = 0;
                        sb.time_ = p->time;
                        sb.device_details_root_ = p->detail_root;
                        sb.data_mapping_root_ = p->mapping_root;
                        sb.metadata_nr_blocks_ = bm->get_nr_blocks();
                }
        }

        superblock_detail::superblock
        recreate_superblock(override_options const &opts)
        {
                superblock_detail::superblock sb;
                memset(&sb, 0, sizeof(sb));

                // FIXME: we need to get this by walking both the mapping and device trees.
                sb.time_ = 100000;


                sb.trans_id_ = opts.get_transaction_id();
                sb.version_ = superblock_detail::METADATA_VERSION;
                sb.data_block_size_ = opts.get_data_block_size();

                // Check that this has been overridden.
                opts.get_nr_data_blocks();

                return sb;
        }

        optional<superblock_detail::superblock>
        maybe_read_superblock(block_manager<>::ptr bm)
        {
                try {
                        auto sb = read_superblock(bm);
                        return optional<superblock_detail::superblock>(sb);
                } catch (...) {
                }

                return optional<superblock_detail::superblock>();
        }

        void
        metadata_repair_(block_manager<>::ptr bm, emitter::ptr e, override_options const &opts)
        {
                auto msb = maybe_read_superblock(bm);
                if (!msb)
                        msb = recreate_superblock(opts);

                auto tm = open_tm(bm, superblock_detail::SUPERBLOCK_LOCATION);

                if (!get_dev_ids(*tm, msb->device_details_root_) ||
                    !get_map_ids(*tm, msb->data_mapping_root_))
                        find_better_roots_(bm, *msb);

                emit_trees_(bm, *msb, e, opts);
        }

}

//----------------------------------------------------------------

void
thin_provisioning::metadata_dump(metadata::ptr md, emitter::ptr e, dump_options const &opts)
{
	details_extractor de(opts);
	device_tree_detail::damage_visitor::ptr dd_policy(details_damage_policy(false));
	walk_device_tree(*md->details_, de, *dd_policy);

	e->begin_superblock("", md->sb_.time_,
			    md->sb_.trans_id_,
			    md->sb_.flags_,
			    md->sb_.version_,
			    md->sb_.data_block_size_,
			    get_nr_blocks(*md),
			    boost::optional<block_address>());

	{
		mapping_tree_detail::damage_visitor::ptr md_policy(mapping_damage_policy(false));
		mapping_tree_emit_visitor mte(opts, *md->tm_, e, de.get_details(), mapping_damage_policy(false));
		walk_mapping_tree(*md->mappings_top_level_, mte, *md_policy);
	}

	e->end_superblock();
}

void
thin_provisioning::metadata_repair(block_manager<>::ptr bm, emitter::ptr e, override_options const &opts)
{
        try {
                metadata_repair_(bm, e, opts);

        } catch (override_error const &e) {
                ostringstream out;
                out << "The following field needs to be provided on the command line due to corruption in the superblock: "
                    << e.what();
                throw runtime_error(out.str());
        }
}

//----------------------------------------------------------------

void
thin_provisioning::metadata_dump_subtree(metadata::ptr md, emitter::ptr e, bool repair, uint64_t subtree_root) {
	mapping_emit_visitor me(e);
	single_mapping_tree tree(*md->tm_, subtree_root,
				 mapping_tree_detail::block_time_ref_counter(md->data_sm_));
	// FIXME: pass the current device id instead of zero
	walk_mapping_tree(tree, 0, static_cast<mapping_tree_detail::mapping_visitor &>(me),
			  *mapping_damage_policy(repair));
}

//----------------------------------------------------------------

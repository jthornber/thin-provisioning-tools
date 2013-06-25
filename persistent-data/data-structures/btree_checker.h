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

#ifndef BTREE_CHECKER_H
#define BTREE_CHECKER_H

#include "btree.h"

#include "persistent-data/block_counter.h"
#include "persistent-data/checksum.h"
#include "persistent-data/error_set.h"

#include <sstream>
#include <map>
#include <set>

using namespace persistent_data;
using namespace std;

//----------------------------------------------------------------

namespace persistent_data {
	//----------------------------------------------------------------
	// This class implements consistency checking for the btrees in
	// general.  Derive from this if you want some additional checks.
	// It's worth summarising what is checked:
	//
	// Implemented
	// -----------
	//
	// - block_nr
	// - nr_entries < max_entries
	// - max_entries fits in block
	// - max_entries is divisible by 3
	// - nr_entries > minimum (except for root nodes)
	//
	// Not implemented
	// ---------------
	//
	// - leaf | internal flags (this can be inferred from siblings)
	//----------------------------------------------------------------
	template <uint32_t Levels, typename ValueTraits>
	class btree_checker : public btree<Levels, ValueTraits>::visitor {
	public:
		typedef btree_detail::node_location node_location;

		btree_checker(block_counter &counter, bool avoid_repeated_visits = true)
			: counter_(counter),
			  errs_(new error_set("btree errors")),
			  avoid_repeated_visits_(avoid_repeated_visits) {
		}

		bool visit_internal(node_location const &loc,
				    btree_detail::node_ref<block_traits> const &n) {
			return check_internal(loc, n);
		}

		bool visit_internal_leaf(node_location const &loc,
					 btree_detail::node_ref<block_traits> const &n) {
			return check_leaf(loc, n);
		}

		bool visit_leaf(node_location const &loc,
				btree_detail::node_ref<ValueTraits> const &n) {
			return check_leaf(loc, n);
		}

		error_set::ptr get_errors() const {
			return errs_;
		}

	protected:
		block_counter &get_counter() {
			return counter_;
		}

	private:
		bool check_internal(node_location const &loc,
				    btree_detail::node_ref<block_traits> const &n) {
			if (!already_visited(n) &&
			    check_block_nr(n) &&
			    check_max_entries(n) &&
			    check_nr_entries(n, loc.is_sub_root()) &&
			    check_ordered_keys(n) &&
			    check_parent_key(loc.is_sub_root() ? boost::optional<uint64_t>() : loc.key, n)) {
				if (loc.is_sub_root())
					new_root(loc.level());

				return true;
			}

			return false;
		}

		template <typename ValueTraits2>
		bool check_leaf(node_location const &loc,
				btree_detail::node_ref<ValueTraits2> const &n) {
			if (!already_visited(n) &&
			    check_block_nr(n) &&
			    check_max_entries(n) &&
			    check_nr_entries(n, loc.is_sub_root()) &&
			    check_ordered_keys(n) &&
			    check_parent_key(loc.is_sub_root() ? boost::optional<uint64_t>() : loc.key, n)) {
				if (loc.is_sub_root())
					new_root(loc.level());

				return check_leaf_key(loc.level(), n);
			}

			return false;
		}


		template <typename node>
		bool already_visited(node const &n) {
			block_address b = n.get_location();

			counter_.inc(b);

			if (avoid_repeated_visits_) {
				if (seen_.count(b) > 0)
					return true;

				seen_.insert(b);
			}

			return false;
		}

		template <typename node>
		bool check_block_nr(node const &n) const {
			if (n.get_location() != n.get_block_nr()) {
				std::ostringstream out;
				out << "block number mismatch: actually "
				    << n.get_location()
				    << ", claims " << n.get_block_nr();
				errs_->add_child(out.str());
				return false;
			}

			return true;
		}

		template <typename node>
		bool check_max_entries(node const &n) const {
			size_t elt_size = sizeof(uint64_t) + n.get_value_size();
			if (elt_size * n.get_max_entries() + sizeof(node_header) > MD_BLOCK_SIZE) {
				std::ostringstream out;
				out << "max entries too large: " << n.get_max_entries();
				errs_->add_child(out.str());
				return false;
			}

			if (n.get_max_entries() % 3) {
				std::ostringstream out;
				out << "max entries is not divisible by 3: " << n.get_max_entries();
				errs_->add_child(out.str());
				return false;
			}

			return true;
		}

		template <typename node>
		bool check_nr_entries(node const &n, bool is_root) const {
			if (n.get_nr_entries() > n.get_max_entries()) {
				std::ostringstream out;
				out << "bad nr_entries: "
				    << n.get_nr_entries() << " < "
				    << n.get_max_entries();
				errs_->add_child(out.str());
				return false;
			}

			block_address min = n.get_max_entries() / 3;
			if (!is_root && (n.get_nr_entries() < min)) {
				ostringstream out;
				out << "too few entries in btree: "
				    << n.get_nr_entries()
				    << ", expected at least "
				    << min
				    << "(max_entries = " << n.get_max_entries() << ")";
				errs_->add_child(out.str());
				return false;
			}

			return true;
		}

		template <typename node>
		bool check_ordered_keys(node const &n) const {
			unsigned nr_entries = n.get_nr_entries();

			if (nr_entries == 0)
				return true; // can only happen if a root node

			uint64_t last_key = n.key_at(0);

			for (unsigned i = 1; i < nr_entries; i++) {
				uint64_t k = n.key_at(i);
				if (k <= last_key) {
					ostringstream out;
					out << "keys are out of order, " << k << " <= " << last_key;
					errs_->add_child(out.str());
					return false;
				}
				last_key = k;
			}

			return true;
		}

		template <typename node>
		bool check_parent_key(boost::optional<uint64_t> key, node const &n) const {
			if (!key)
				return true;

			if (*key > n.key_at(0)) {
				ostringstream out;
				out << "parent key mismatch: parent was " << *key
				    << ", but lowest in node was " << n.key_at(0);
				errs_->add_child(out.str());
				return false;
			}

			return true;
		}

		template <typename node>
		bool check_leaf_key(unsigned level, node const &n) {
			if (n.get_nr_entries() == 0)
				return true; // can only happen if a root node

			if (last_leaf_key_[level] && *last_leaf_key_[level] >= n.key_at(0)) {
				ostringstream out;
				out << "the last key of the previous leaf was " << *last_leaf_key_[level]
				    << " and the first key of this leaf is " << n.key_at(0);
				errs_->add_child(out.str());
				return false;
			}

			last_leaf_key_[level] = n.key_at(n.get_nr_entries() - 1);
			return true;
		}

     	        void new_root(unsigned level) {
			// we're starting a new subtree, so should
			// reset the last_leaf value.
			last_leaf_key_[level] = boost::optional<uint64_t>();
	        }

		block_counter &counter_;
		std::set<block_address> seen_;
		error_set::ptr errs_;
		boost::optional<uint64_t> last_leaf_key_[Levels];
		bool avoid_repeated_visits_;
	};
}

//----------------------------------------------------------------

#endif

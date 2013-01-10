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

#include "persistent-data/transaction_manager.h"
#include "persistent-data/space-maps/core.h"
#include "persistent-data/btree.h"

#define BOOST_TEST_MODULE BTreeTests
#include <boost/test/included/unit_test.hpp>

using namespace std;
using namespace boost;
using namespace persistent_data;

//----------------------------------------------------------------

namespace {
	block_address const NR_BLOCKS = 102400;

	transaction_manager::ptr
	create_tm() {
		block_manager<>::ptr bm(new block_manager<>("./test.data", NR_BLOCKS, 4, true));
		space_map::ptr sm(new core_map(NR_BLOCKS));
		transaction_manager::ptr tm(new transaction_manager(bm, sm));
		return tm;
	}

	btree<1, uint64_traits>::ptr
	create_btree() {
		uint64_traits::ref_counter rc;

		return btree<1, uint64_traits>::ptr(
			new btree<1, uint64_traits>(create_tm(), rc));
	}

	// Checks that a btree is well formed.
	//
	// i) No block should be in the tree more than once.
	//
	class constraint_visitor : public btree<1, uint64_traits>::visitor {
	public:
		typedef btree_detail::node_ref<uint64_traits> internal_node;
		typedef btree_detail::node_ref<uint64_traits> leaf_node;

		bool visit_internal(unsigned level, bool sub_root,
				    boost::optional<uint64_t> key,
				    internal_node const &n) {
			check_duplicate_block(n.get_location());
			return true;
		}

	  bool visit_internal_leaf(unsigned level, bool sub_root,
				   boost::optional<uint64_t> key,
				   internal_node const &n) {
			check_duplicate_block(n.get_location());
			return true;
		}

	  bool visit_leaf(unsigned level, bool sub_root,
			  boost::optional<uint64_t> key,
			  leaf_node const &n) {
			check_duplicate_block(n.get_location());
			return true;
		}

	private:
		void check_duplicate_block(block_address b) {
			if (seen_.count(b)) {
				ostringstream out;
				out << "duplicate block in btree: " << b;
				throw runtime_error(out.str());
			}

			seen_.insert(b);
		}

		set<block_address> seen_;
	};

	void check_constraints(btree<1, uint64_traits>::ptr tree) {
		typedef btree<1, uint64_traits> tree_type;

		tree_type::visitor::ptr v(new constraint_visitor);
		tree->visit(v);
	}
}

//----------------------------------------------------------------

BOOST_AUTO_TEST_CASE(empty_btree_contains_nothing)
{
	btree<1, uint64_traits>::ptr tree = create_btree();
	check_constraints(tree);

	for (uint64_t i = 0; i < 1000; i++) {
		uint64_t key[1] = {i};
		BOOST_CHECK(!tree->lookup(key));
	}
}

BOOST_AUTO_TEST_CASE(insert_works)
{
	unsigned const COUNT = 100000;

	btree<1, uint64_traits>::ptr tree = create_btree();
	for (uint64_t i = 0; i < COUNT; i++) {
		uint64_t key[1] = {i * 7};
		uint64_t value = i;

		tree->insert(key, value);

		btree<1, uint64_traits>::maybe_value l = tree->lookup(key);
		BOOST_REQUIRE(l);
		BOOST_CHECK_EQUAL(*l, i);
	}

	check_constraints(tree);
}

BOOST_AUTO_TEST_CASE(insert_does_not_insert_imaginary_values)
{
	btree<1, uint64_traits>::ptr tree = create_btree();
	uint64_t key[1] = {0};
	uint64_t value = 100;

	btree<1, uint64_traits>::maybe_value l = tree->lookup(key);
	BOOST_CHECK(!l);

	key[0] = 1;
	l = tree->lookup(key);
	BOOST_CHECK(!l);

	key[0] = 0;
	tree->insert(key, value);

	l = tree->lookup(key);
	BOOST_REQUIRE(l);
	BOOST_CHECK_EQUAL(*l, 100);

	key[0] = 1;
	l = tree->lookup(key);
	BOOST_CHECK(!l);

	check_constraints(tree);
}

//----------------------------------------------------------------

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

#include "gmock/gmock.h"
#include "persistent-data/transaction_manager.h"
#include "persistent-data/space-maps/core.h"
#include "persistent-data/data-structures/btree.h"
#include "persistent-data/data-structures/simple_traits.h"

using namespace std;
using namespace persistent_data;
using namespace testing;

//----------------------------------------------------------------

namespace {
	block_address const NR_BLOCKS = 102400;

	class BtreeTests : public Test {
	public:
		BtreeTests() 
			: bm_(new block_manager<>("./test.data", NR_BLOCKS, 4, block_manager<>::READ_WRITE)),
			  sm_(new core_map(NR_BLOCKS)),
			  tm_(bm_, sm_) {
		}

		btree<1, uint64_traits>::ptr
		create_btree() {
			uint64_traits::ref_counter rc;

			return btree<1, uint64_traits>::ptr(
				new btree<1, uint64_traits>(tm_, rc));
		}

	private:
		block_manager<>::ptr bm_;
		space_map::ptr sm_;
		transaction_manager tm_;
	};


	// Checks that a btree is well formed.
	//
	// i) No block should be in the tree more than once.
	//
	class constraint_visitor : public btree<1, uint64_traits>::visitor {
	public:
		typedef btree_detail::node_location node_location;
		typedef btree_detail::node_ref<block_traits> internal_node;
		typedef btree_detail::node_ref<uint64_traits> leaf_node;

		bool visit_internal(node_location const &loc,
				    internal_node const &n) {
			check_duplicate_block(n.get_location());
			return true;
		}

		bool visit_internal_leaf(node_location const &loc,
					 internal_node const &n) {
			check_duplicate_block(n.get_location());
			return true;
		}

		bool visit_leaf(node_location const &loc,
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

		constraint_visitor v;
		tree->visit_depth_first(v);
	}
}

//----------------------------------------------------------------

TEST_F(BtreeTests, empty_btree_contains_nothing)
{
	btree<1, uint64_traits>::ptr tree = create_btree();
	check_constraints(tree);

	for (uint64_t i = 0; i < 1000; i++) {
		uint64_t key[1] = {i};
		ASSERT_FALSE(tree->lookup(key));
	}
}

TEST_F(BtreeTests, insert_works)
{
	unsigned const COUNT = 100000;

	btree<1, uint64_traits>::ptr tree = create_btree();
	for (uint64_t i = 0; i < COUNT; i++) {
		uint64_t key[1] = {i * 7};
		uint64_t value = i;

		tree->insert(key, value);

		btree<1, uint64_traits>::maybe_value l = tree->lookup(key);
		ASSERT_TRUE(l);
		ASSERT_THAT(*l, Eq(i));
	}

	check_constraints(tree);
}

TEST_F(BtreeTests, insert_does_not_insert_imaginary_values)
{
	btree<1, uint64_traits>::ptr tree = create_btree();
	uint64_t key[1] = {0};
	uint64_t value = 100;

	btree<1, uint64_traits>::maybe_value l = tree->lookup(key);
	ASSERT_FALSE(l);

	key[0] = 1;
	l = tree->lookup(key);
	ASSERT_FALSE(l);

	key[0] = 0;
	tree->insert(key, value);

	l = tree->lookup(key);
	ASSERT_TRUE(l);
	ASSERT_THAT(*l, Eq(100u));

	key[0] = 1;
	l = tree->lookup(key);
	ASSERT_FALSE(l);

	check_constraints(tree);
}

TEST_F(BtreeTests, clone)
{
	typedef btree<1, uint64_traits> tree64;

	unsigned const COUNT = 1000;

	tree64::maybe_value l;
	tree64::ptr tree = create_btree();
	for (uint64_t i = 0; i < COUNT; i++) {
		uint64_t key[1] = {i};
		uint64_t value = i * 7;

		tree->insert(key, value);
	}

	for (uint64_t i = 0; i < COUNT; i++) {
		uint64_t key[1] = {i};
		uint64_t value = i * 7;

		l = tree->lookup(key);
		ASSERT_TRUE(l);
		ASSERT_THAT(*l, Eq(value));
	}

	tree64::ptr copy = tree->clone();
	for (uint64_t i = 0; i < COUNT; i++) {
		uint64_t key[1] = {i + COUNT};
		uint64_t value = (i + COUNT) * 7;

		copy->insert(key, value);
	}

	for (uint64_t i = 0; i < COUNT; i++) {
		uint64_t key[1] = {i};
		uint64_t value = i * 7;

		l = tree->lookup(key);
		ASSERT_TRUE(l);
		ASSERT_THAT(*l, Eq(value));

		l = copy->lookup(key);
		ASSERT_TRUE(l);
		ASSERT_THAT(*l, Eq(value));
	}

	for (uint64_t i = 0; i < COUNT; i++) {
		uint64_t key[1] = {i + COUNT};
		uint64_t value = (i + COUNT) * 7;

		l = tree->lookup(key);
		ASSERT_FALSE(l);

		l = copy->lookup(key);
		ASSERT_TRUE(l);
		ASSERT_THAT(*l, Eq(value));
	}

	check_constraints(tree);
}

//----------------------------------------------------------------

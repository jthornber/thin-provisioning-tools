// Copyright (C) 2013 Red Hat, Inc. All rights reserved.
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
#include "persistent-data/data-structures/bitset.h"

#include <vector>

using namespace std;
using namespace persistent_data;
using namespace testing;

//----------------------------------------------------------------

namespace {
	block_address const NR_BLOCKS = 102400;

	class bitset_checker : public bitset_detail::bitset_visitor {
	public:
		bitset_checker(unsigned size, unsigned m)
			: size_(size), m_(m) {
		}

		void visit(uint32_t index, bool value) {
			ASSERT_THAT(index, Lt(size_));
			ASSERT_THAT(value, Eq(index % 7 ? true : false));
		}

		void visit(bitset_detail::missing_bits const &d) {
			// we aren't expecting any damage
			FAIL();
		}

	private:
		unsigned size_, m_;
	};

	class BitsetTests : public Test {
	public:
		BitsetTests()
			: bm_(new block_manager<>("./test.data", NR_BLOCKS, 4, block_manager<>::READ_WRITE)),
			  sm_(new core_map(NR_BLOCKS)),
			  tm_(bm_, sm_) {
		}

		bitset::ptr
		create_bitset() {
			return bitset::ptr(new bitset(tm_));
		}

		bitset::ptr
		open_bitset(block_address root, unsigned count) {
			return bitset::ptr(new bitset(tm_, root, count));
		}

	private:
		block_manager<>::ptr bm_;
		space_map::ptr sm_;
		transaction_manager tm_;
	};
}

//----------------------------------------------------------------

TEST_F(BitsetTests, create_empty_bitset)
{
	bitset::ptr bs = create_bitset();
	ASSERT_THROW(bs->get(0), runtime_error);
}

TEST_F(BitsetTests, grow_default_false)
{
	unsigned const COUNT = 100000;

	bitset::ptr bs = create_bitset();
	bs->grow(COUNT, false);

	for (unsigned i = 0; i < COUNT; i++)
		ASSERT_FALSE(bs->get(i));
}

TEST_F(BitsetTests, grow_default_true)
{
	unsigned const COUNT = 100000;

	bitset::ptr bs = create_bitset();
	bs->grow(COUNT, true);

	for (unsigned i = 0; i < COUNT; i++)
		ASSERT_TRUE(bs->get(i));
}

TEST_F(BitsetTests, grow_throws_if_actualy_asked_to_shrink)
{
	unsigned const COUNT = 100000;

	bitset::ptr bs = create_bitset();
	bs->grow(COUNT, false);
	ASSERT_THROW(bs->grow(COUNT / 2, false), runtime_error);
}

TEST_F(BitsetTests, multiple_grow_calls)
{
	unsigned const COUNT = 100000;
	unsigned const STEP = 37;
	bitset::ptr bs = create_bitset();

	vector<unsigned> chunks;
	unsigned c;
	for (c = 0; c < COUNT; c += STEP)
		chunks.push_back(c);
	chunks.push_back(c);

	bool default_value = true;
	for (unsigned i = 1; i < chunks.size(); i++) {
		bs->grow(chunks[i], default_value);

		for (unsigned j = chunks[i - 1]; j < chunks[i]; j++)
			ASSERT_THAT(bs->get(j), Eq(default_value));

		default_value = !default_value;
	}

	default_value = true;
	for (unsigned i = 1; i < chunks.size(); i++) {
		for (unsigned j = chunks[i - 1]; j < chunks[i]; j++)
			ASSERT_THAT(bs->get(j), Eq(default_value));

		default_value = !default_value;
	}
}

TEST_F(BitsetTests, set_out_of_bounds_throws)
{
	unsigned const COUNT = 100000;
	bitset::ptr bs = create_bitset();

	ASSERT_THROW(bs->set(0, true), runtime_error);
	bs->grow(COUNT, true);
	ASSERT_THROW(bs->set(COUNT, true), runtime_error);
}

TEST_F(BitsetTests, set_works)
{
	unsigned const COUNT = 100000;
	bitset::ptr bs = create_bitset();

	bs->grow(COUNT, true);
	for (unsigned i = 0; i < COUNT; i += 7)
		bs->set(i, false);

	for (unsigned i = 0; i < COUNT; i++)
		ASSERT_THAT(bs->get(i), Eq(i % 7 ? true : false));
}

TEST_F(BitsetTests, reopen_works)
{
	unsigned const COUNT = 100001;
	block_address root;

	{
		bitset::ptr bs = create_bitset();

		bs->grow(COUNT, true);
		for (unsigned i = 0; i < COUNT; i += 7)
			bs->set(i, false);

		root = bs->get_root();
	}

	{
		bitset::ptr bs = open_bitset(root, COUNT);
		for (unsigned i = 0; i < COUNT; i++)
			ASSERT_THAT(bs->get(i), Eq(i % 7 ? true : false));
	}
}

TEST_F(BitsetTests, walk_bitset)
{
	unsigned const COUNT = 100001;
	block_address root;

	{
		bitset::ptr bs = create_bitset();

		bs->grow(COUNT, true);
		for (unsigned i = 0; i < COUNT; i += 7)
			bs->set(i, false);

		root = bs->get_root();

		bitset_checker c(COUNT, 7);
		bs->walk_bitset(c);
	}

	{
		bitset::ptr bs = open_bitset(root, COUNT);
		bitset_checker c(COUNT, 7);
		bs->walk_bitset(c);
	}
}

//----------------------------------------------------------------

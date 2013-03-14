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

#include "persistent-data/transaction_manager.h"
#include "persistent-data/space-maps/core.h"
#include "persistent-data/data-structures/bitset.h"

#include <vector>

#define BOOST_TEST_MODULE ArrayTests
#include <boost/test/included/unit_test.hpp>

using namespace std;
using namespace boost;
using namespace persistent_data;

//----------------------------------------------------------------

namespace {
	block_address const NR_BLOCKS = 102400;

	transaction_manager::ptr
	create_tm() {
		block_manager<>::ptr bm(new block_manager<>("./test.data", NR_BLOCKS, 4, block_io<>::READ_WRITE));
		space_map::ptr sm(new core_map(NR_BLOCKS));
		transaction_manager::ptr tm(new transaction_manager(bm, sm));
		return tm;
	}

	bitset::ptr
	create_bitset() {
		return bitset::ptr(new bitset(create_tm()));
	}

	bitset::ptr
	open_bitset(block_address root, unsigned count) {
		return bitset::ptr(new bitset(create_tm(), root, count));
	}
}

//----------------------------------------------------------------

BOOST_AUTO_TEST_CASE(create_empty_bitset)
{
	bitset::ptr bs = create_bitset();
	BOOST_CHECK_THROW(bs->get(0), runtime_error);
}

BOOST_AUTO_TEST_CASE(grow_default_false)
{
	unsigned const COUNT = 100000;

	bitset::ptr bs = create_bitset();
	bs->grow(COUNT, false);

	for (unsigned i = 0; i < COUNT; i++)
		BOOST_CHECK_EQUAL(bs->get(i), false);
}

BOOST_AUTO_TEST_CASE(grow_default_true)
{
	unsigned const COUNT = 100000;

	bitset::ptr bs = create_bitset();
	bs->grow(COUNT, true);

	for (unsigned i = 0; i < COUNT; i++)
		BOOST_CHECK_EQUAL(bs->get(i), true);
}

BOOST_AUTO_TEST_CASE(grow_throws_if_actualy_asked_to_shrink)
{
	unsigned const COUNT = 100000;

	bitset::ptr bs = create_bitset();
	bs->grow(COUNT, false);
	BOOST_CHECK_THROW(bs->grow(COUNT / 2, false), runtime_error);
}

BOOST_AUTO_TEST_CASE(multiple_grow_calls)
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
			BOOST_CHECK_EQUAL(bs->get(j), default_value);

		default_value = !default_value;
	}

	default_value = true;
	for (unsigned i = 1; i < chunks.size(); i++) {
		for (unsigned j = chunks[i - 1]; j < chunks[i]; j++)
			BOOST_CHECK_EQUAL(bs->get(j), default_value);

		default_value = !default_value;
	}
}

BOOST_AUTO_TEST_CASE(set_out_of_bounds_throws)
{
	unsigned const COUNT = 100000;
	bitset::ptr bs = create_bitset();

	BOOST_CHECK_THROW(bs->set(0, true), runtime_error);
	bs->grow(COUNT, true);
	BOOST_CHECK_THROW(bs->set(COUNT, true), runtime_error);
}

BOOST_AUTO_TEST_CASE(set_works)
{
	unsigned const COUNT = 100000;
	bitset::ptr bs = create_bitset();

	bs->grow(COUNT, true);
	for (unsigned i = 0; i < COUNT; i += 7)
		bs->set(i, false);

	for (unsigned i = 0; i < COUNT; i++) {
		BOOST_CHECK_EQUAL(bs->get(i), i % 7 ? true : false);
	}
}

BOOST_AUTO_TEST_CASE(reopen_works)
{
	unsigned const COUNT = 100000;
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
			BOOST_CHECK_EQUAL(bs->get(i), i % 7 ? true : false);
	}
}

//----------------------------------------------------------------

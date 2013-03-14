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
#include "persistent-data/data-structures/array.h"

#include <vector>

#define BOOST_TEST_MODULE ArrayTests
#include <boost/test/included/unit_test.hpp>

using namespace std;
using namespace boost;
using namespace persistent_data;

//----------------------------------------------------------------

namespace {
	block_address const NR_BLOCKS = 102400;
	typedef array<uint64_traits> array64;

	transaction_manager::ptr
	create_tm() {
		block_manager<>::ptr bm(new block_manager<>("./test.data", NR_BLOCKS, 4, block_io<>::READ_WRITE));
		space_map::ptr sm(new core_map(NR_BLOCKS));
		transaction_manager::ptr tm(new transaction_manager(bm, sm));
		return tm;
	}

	typename array64::ptr
	create_array(unsigned nr_entries, uint64_t default_value) {
		uint64_traits::ref_counter rc;

		typename array64::ptr a(new array64(create_tm(), rc));

		if (nr_entries)
			a->grow(nr_entries, default_value);

		return a;
	}

	typename array64::ptr
	open_array(block_address root, unsigned nr_entries) {
		uint64_traits::ref_counter rc;

		typename array64::ptr a(new array64(create_tm(), rc, root, nr_entries));
		return a;
	}
}

//----------------------------------------------------------------

BOOST_AUTO_TEST_CASE(can_create_an_empty_array)
{
	array<uint64_traits>::ptr tree = create_array(0, 0);

	BOOST_CHECK_THROW(tree->get(0), runtime_error);
}

BOOST_AUTO_TEST_CASE(get_elements)
{
	unsigned const COUNT = 10000;
	array<uint64_traits>::ptr tree = create_array(COUNT, 123);

	for (unsigned i = 0; i < COUNT; i++) {
		BOOST_CHECK_EQUAL(tree->get(i), 123);
	}

	BOOST_CHECK_THROW(tree->get(COUNT), runtime_error);
}

BOOST_AUTO_TEST_CASE(set_elements)
{
	unsigned const COUNT = 10000;
	array<uint64_traits>::ptr tree = create_array(COUNT, 123);

	for (unsigned i = 0; i < COUNT; i++)
		tree->set(i, 124);

	for (unsigned i = 0; i < COUNT; i++)
		BOOST_CHECK_EQUAL(tree->get(i), 124);

	BOOST_CHECK_THROW(tree->get(COUNT), runtime_error);
}

template <typename T, unsigned size>
unsigned array_size(T (&)[size]) {
	return size;
}

BOOST_AUTO_TEST_CASE(grow)
{
	unsigned const COUNT = 10000;
	unsigned const STEPS[] = {
		17, 71, 137, 277, 439, 683, 967
	};

	for (unsigned s = 0; s < array_size(STEPS); s++) {

		unsigned step = STEPS[s];

		vector<unsigned> chunks;
		for (unsigned c = 0; c < COUNT; c += step)
			chunks.push_back(c);
		chunks.push_back(COUNT);

		array<uint64_traits>::ptr a = create_array(0, 123);

		for (unsigned i = 1; i < chunks.size(); i++) {
			if (i > 1)
				BOOST_CHECK_EQUAL(a->get(chunks[i - 1] - 1), i - 1);

			a->grow(chunks[i], i);

			if (i > 1)
				BOOST_CHECK_EQUAL(a->get(chunks[i - 1] - 1), i - 1);

			for (unsigned j = chunks[i - 1]; j < chunks[i]; j++)
				BOOST_CHECK_EQUAL(a->get(j), i);

			BOOST_CHECK_THROW(a->get(chunks[i] + 1), runtime_error);
		}
	}
}

BOOST_AUTO_TEST_CASE(reopen_array)
{
	unsigned const COUNT = 10000;
	block_address root;

	{
		typename array64::ptr a = create_array(COUNT, 123);

		for (unsigned i = 0; i < COUNT; i += 7)
			a->set(i, 234);

		root = a->get_root();
	}

	{
		typename array64::ptr a = open_array(root, COUNT);

		for (unsigned i = 0; i < COUNT; i++)
			BOOST_CHECK_EQUAL(a->get(i), i % 7 ? 123: 234);
	}
}

BOOST_AUTO_TEST_CASE(destroy)
{
	// FIXME: pending
}

//----------------------------------------------------------------

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

#include "persistent-data/space_map_disk.h"
#include "persistent-data/space_map_core.h"
#include "persistent-data/space_map_transactional.h"

#define BOOST_TEST_MODULE SpaceMapDiskTests
#include <boost/test/included/unit_test.hpp>

using namespace std;
using namespace boost;
using namespace persistent_data;

//----------------------------------------------------------------

namespace {
	block_address const NR_BLOCKS = 1000; // FIXME: bump up
	block_address const SUPERBLOCK = 0;
	block_address const MAX_LOCKS = 8;

	transaction_manager::ptr
	create_tm() {
		block_manager<>::ptr bm(
			new block_manager<>("./test.data", NR_BLOCKS, MAX_LOCKS, true));
		space_map::ptr sm(new core_map(NR_BLOCKS));
		transaction_manager::ptr tm(
			new transaction_manager(bm, sm));
		return tm;
	}

	class sm_core_creator {
	public:
		static space_map::ptr
		create() {
			return space_map::ptr(new persistent_data::core_map(NR_BLOCKS));
		}
	};

	class sm_transactional_creator {
	public:
		static space_map::ptr
		create() {
			return create_transactional_sm(
				checked_space_map::ptr(
					new core_map(NR_BLOCKS)));
		}
	};

	class sm_disk_creator {
	public:
		static persistent_space_map::ptr
		create() {
			transaction_manager::ptr tm = create_tm();
			return persistent_data::create_disk_sm(tm, NR_BLOCKS);
		}

		static persistent_space_map::ptr
		open(void *root) {
			transaction_manager::ptr tm = create_tm();
			return persistent_data::open_disk_sm(tm, root);
		}
	};

	class sm_metadata_creator {
	public:
		static persistent_space_map::ptr
		create() {
			transaction_manager::ptr tm = create_tm();
			return persistent_data::create_metadata_sm(tm, NR_BLOCKS);
		}

		static persistent_space_map::ptr
		open(void *root) {
			transaction_manager::ptr tm = create_tm();
			return persistent_data::open_metadata_sm(tm, root);
		}
	};

	//--------------------------------

	void test_get_nr_blocks(space_map::ptr sm)
	{
		BOOST_CHECK_EQUAL(sm->get_nr_blocks(), NR_BLOCKS);
	}

	void test_get_nr_free(space_map::ptr sm)
	{
		BOOST_REQUIRE_EQUAL(sm->get_nr_free(), NR_BLOCKS);

		for (unsigned i = 0; i < NR_BLOCKS; i++) {
			optional<block_address> mb = sm->new_block();
			BOOST_REQUIRE(mb);
			BOOST_REQUIRE_EQUAL(sm->get_nr_free(), NR_BLOCKS - i - 1);
		}

		for (unsigned i = 0; i < NR_BLOCKS; i++) {
			sm->dec(i);
			BOOST_REQUIRE_EQUAL(sm->get_nr_free(), i + 1);
		}
	}

	void test_runs_out_of_space(space_map::ptr sm)
	{
		optional<block_address> mb;

		for (unsigned i = 0; i < NR_BLOCKS; i++)
			mb = sm->new_block();

		mb = sm->new_block();
		BOOST_REQUIRE(!mb);
	}

	void test_inc_and_dec(space_map::ptr sm)
	{
		block_address b = 63;

		for (unsigned i = 0; i < 50; i++) {
			BOOST_REQUIRE_EQUAL(sm->get_count(b), i);
			sm->inc(b);
		}

		for (unsigned i = 50; i > 0; i--) {
			BOOST_REQUIRE_EQUAL(sm->get_count(b), i);
			sm->dec(b);
		}
	}

	void test_not_allocated_twice(space_map::ptr sm)
	{
		optional<block_address> mb = sm->new_block();
		BOOST_REQUIRE(mb);

		for (;;) {
			boost::optional<block_address> b = sm->new_block();
			if (!b)
				break;

			if (b)
				BOOST_REQUIRE(*b != *mb);
		}
	}

	void test_set_count(space_map::ptr sm)
	{
		sm->set_count(43, 5);
		BOOST_REQUIRE_EQUAL(sm->get_count(43), 5);
	}

	void test_set_affects_nr_allocated(space_map::ptr sm)
	{
		for (unsigned i = 0; i < NR_BLOCKS; i++) {
			sm->set_count(i, 1);
			BOOST_REQUIRE_EQUAL(sm->get_nr_free(), NR_BLOCKS - i - 1);
		}

		for (unsigned i = 0; i < NR_BLOCKS; i++) {
			sm->set_count(i, 0);
			BOOST_REQUIRE_EQUAL(sm->get_nr_free(), i + 1);
		}
	}

	// Ref counts below 3 gets stored as bitmaps, above 3 they go into
	// a btree with uint32_t values.  Worth checking this thoroughly,
	// especially for the metadata format which may have complications
	// due to recursion.
	void test_high_ref_counts(space_map::ptr sm)
	{
		srand(1234);
		for (unsigned i = 0; i < NR_BLOCKS; i++)
			sm->set_count(i, rand() % 6789);
		sm->commit();

		for (unsigned i = 0; i < NR_BLOCKS; i++) {
			sm->inc(i);
			sm->inc(i);
			if (i % 1000)
				sm->commit();
		}
		sm->commit();

		srand(1234);
		for (unsigned i = 0; i < NR_BLOCKS; i++)
			BOOST_REQUIRE_EQUAL(sm->get_count(i), (rand() % 6789) + 2);

		for (unsigned i = 0; i < NR_BLOCKS; i++)
			sm->dec(i);

		srand(1234);
		for (unsigned i = 0; i < NR_BLOCKS; i++)
			BOOST_REQUIRE_EQUAL(sm->get_count(i), (rand() % 6789) + 1);
	}

	template <typename SMCreator>
	void test_sm_reopen()
	{
		unsigned char buffer[128];

		{
			persistent_space_map::ptr sm = SMCreator::create();
			for (unsigned i = 0, step = 1; i < NR_BLOCKS; i += step, step++)
				sm->inc(i);
			sm->commit();

			BOOST_REQUIRE(sm->root_size() <= sizeof(buffer));
			sm->copy_root(buffer, sizeof(buffer));
		}

		{
			persistent_space_map::ptr sm = SMCreator::open(buffer);

			for (unsigned i = 0, step = 1; i < NR_BLOCKS; i += step, step++)
				BOOST_REQUIRE_EQUAL(sm->get_count(i), 1);
		}
	}

	typedef void (*sm_test)(space_map::ptr);

	template <typename SMCreator, unsigned NTests>
	void do_tests(sm_test (&tests)[NTests])
	{
		for (unsigned t = 0; t < NTests; t++) {
			space_map::ptr sm = SMCreator::create();
			tests[t](sm);
		}
	}

	sm_test space_map_tests[] = {
		test_get_nr_blocks,
		test_get_nr_free,
		test_runs_out_of_space,
		test_inc_and_dec,
		test_not_allocated_twice,
		test_set_count,
		test_set_affects_nr_allocated,
		test_high_ref_counts
	};
}

//----------------------------------------------------------------

BOOST_AUTO_TEST_CASE(test_sm_core)
{
	do_tests<sm_core_creator>(space_map_tests);
}

BOOST_AUTO_TEST_CASE(test_sm_transactional)
{
	do_tests<sm_transactional_creator>(space_map_tests);
}

BOOST_AUTO_TEST_CASE(test_sm_disk)
{
	do_tests<sm_disk_creator>(space_map_tests);
	test_sm_reopen<sm_disk_creator>();

}

BOOST_AUTO_TEST_CASE(test_sm_metadata)
{
	do_tests<sm_metadata_creator>(space_map_tests);
	test_sm_reopen<sm_metadata_creator>();
}

//----------------------------------------------------------------

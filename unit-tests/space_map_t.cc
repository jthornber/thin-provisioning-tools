// Copyright (C) 2011-2012 Red Hat, Inc. All rights reserved.
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
#include "persistent-data/space-maps/disk.h"
#include "persistent-data/space-maps/core.h"
#include "persistent-data/space-maps/careful_alloc.h"
#include "persistent-data/space-maps/recursive.h"

using namespace std;
using namespace persistent_data;
using namespace testing;

//----------------------------------------------------------------

namespace {
	block_address const NR_BLOCKS = 1000; // FIXME: bump up
	block_address const SUPERBLOCK = 0;
	block_address const MAX_LOCKS = 8;

	class SpaceMapTests : public Test {
	public:
		SpaceMapTests()
			: bm_(new block_manager<>("./test.data", NR_BLOCKS, MAX_LOCKS, block_manager<>::READ_WRITE)),
			  sm_(new core_map(NR_BLOCKS)),
			  tm_(bm_, sm_) {
		}

		struct sm_core_creator {
			static space_map::ptr
			create(transaction_manager &tm) {
				return space_map::ptr(new persistent_data::core_map(NR_BLOCKS));
			}
		};

		struct sm_careful_alloc_creator {
			static space_map::ptr
			create(transaction_manager &tm) {
				return create_careful_alloc_sm(
					checked_space_map::ptr(
						new core_map(NR_BLOCKS)));
			}
		};

		struct sm_recursive_creator {
			static checked_space_map::ptr
			create(transaction_manager &tm) {
				return create_recursive_sm(
					checked_space_map::ptr(
						new core_map(NR_BLOCKS)));
			}
		};

		struct sm_disk_creator {
			static persistent_space_map::ptr
			create(transaction_manager &tm) {
				return persistent_data::create_disk_sm(tm, NR_BLOCKS);
			}

			static persistent_space_map::ptr
			open(transaction_manager &tm, void *root) {
				return persistent_data::open_disk_sm(tm, root);
			}
		};

		struct sm_metadata_creator {
			static persistent_space_map::ptr
			create(transaction_manager &tm) {
				return persistent_data::create_metadata_sm(tm, NR_BLOCKS);
			}

			static persistent_space_map::ptr
			open(transaction_manager &tm, void *root) {
				return persistent_data::open_metadata_sm(tm, root);
			}
		};

		//--------------------------------

		void test_get_nr_blocks(space_map::ptr sm) {
			ASSERT_THAT(sm->get_nr_blocks(), Eq(NR_BLOCKS));
		}

		void test_get_nr_free(space_map::ptr sm) {
			ASSERT_THAT(sm->get_nr_free(), Eq(NR_BLOCKS));

			for (unsigned i = 0; i < NR_BLOCKS; i++) {
				boost::optional<block_address> mb = sm->new_block();
				ASSERT_TRUE(mb);
				ASSERT_THAT(sm->get_nr_free(), Eq(NR_BLOCKS - i - 1));
			}

			for (unsigned i = 0; i < NR_BLOCKS; i++) {
				sm->dec(i);
				ASSERT_THAT(sm->get_nr_free(), Eq(i + 1));
			}
		}

		void test_runs_out_of_space(space_map::ptr sm) {
			boost::optional<block_address> mb;

			for (unsigned i = 0; i < NR_BLOCKS; i++)
				mb = sm->new_block();

			mb = sm->new_block();
			ASSERT_FALSE(mb);
		}

		void test_inc_and_dec(space_map::ptr sm) {
			block_address b = 63;

			for (unsigned i = 0; i < 50; i++) {
				ASSERT_THAT(sm->get_count(b), Eq(i));
				sm->inc(b);
			}

			for (unsigned i = 50; i > 0; i--) {
				ASSERT_THAT(sm->get_count(b), Eq(i));
				sm->dec(b);
			}
		}

		void test_not_allocated_twice(space_map::ptr sm) {
			boost::optional<block_address> mb = sm->new_block();
			ASSERT_TRUE(mb);

			for (;;) {
				boost::optional<block_address> b = sm->new_block();
				if (!b)
					break;

				if (b)
					ASSERT_TRUE(*b != *mb);
			}
		}

		void test_set_count(space_map::ptr sm) {
			sm->set_count(43, 5);
			ASSERT_THAT(sm->get_count(43), Eq(5u));
		}

		void test_set_affects_nr_allocated(space_map::ptr sm) {
			for (unsigned i = 0; i < NR_BLOCKS; i++) {
				sm->set_count(i, 1);
				ASSERT_THAT(sm->get_nr_free(), Eq(NR_BLOCKS - i - 1));
			}

			for (unsigned i = 0; i < NR_BLOCKS; i++) {
				sm->set_count(i, 0);
				ASSERT_THAT(sm->get_nr_free(), Eq(i + 1));
			}
		}

		// Ref counts below 3 gets stored as bitmaps, above 3 they go into
		// a btree with uint32_t values.  Worth checking this thoroughly,
		// especially for the metadata format which may have complications
		// due to recursion.
		void test_high_ref_counts(space_map::ptr sm) {
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
				ASSERT_THAT(sm->get_count(i), Eq((rand() % 6789u) + 2u));

			for (unsigned i = 0; i < NR_BLOCKS; i++)
				sm->dec(i);

			srand(1234);
			for (unsigned i = 0; i < NR_BLOCKS; i++)
				ASSERT_THAT(sm->get_count(i), Eq((rand() % 6789u) + 1u));
		}

		template <typename SMCreator>
		void test_sm_reopen() {
			unsigned char buffer[128];

			{
				persistent_space_map::ptr sm = SMCreator::create(tm_);
				for (unsigned i = 0, step = 1; i < NR_BLOCKS; i += step, step++)
					sm->inc(i);
				sm->commit();

				ASSERT_THAT(sm->root_size(), Le(sizeof(buffer)));
				sm->copy_root(buffer, sizeof(buffer));
			}

			{
				persistent_space_map::ptr sm = SMCreator::open(tm_, buffer);

				for (unsigned i = 0, step = 1; i < NR_BLOCKS; i += step, step++)
					ASSERT_THAT(sm->get_count(i), Eq(1u));
			}
		}

		template <typename SMCreator>
		void do_tests() {
			test_get_nr_blocks(SMCreator::create(tm_));
			test_get_nr_free(SMCreator::create(tm_));
			test_runs_out_of_space(SMCreator::create(tm_));
			test_inc_and_dec(SMCreator::create(tm_));
			test_not_allocated_twice(SMCreator::create(tm_));
			test_set_count(SMCreator::create(tm_));
			test_set_affects_nr_allocated(SMCreator::create(tm_));
			test_high_ref_counts(SMCreator::create(tm_));
		}

		void
		copy_space_maps(space_map::ptr lhs, space_map::ptr rhs) {
			for (block_address b = 0; b < rhs->get_nr_blocks(); b++) {
				uint32_t count = rhs->get_count(b);
				if (count > 0)
					lhs->set_count(b, rhs->get_count(b));
			}
		}

		block_manager<>::ptr bm_;
		space_map::ptr sm_;
		transaction_manager tm_;
	};
}

//----------------------------------------------------------------

TEST_F(SpaceMapTests, test_sm_core)
{
	do_tests<sm_core_creator>();
}

TEST_F(SpaceMapTests, test_sm_careful_alloc)
{
	do_tests<sm_careful_alloc_creator>();
}

TEST_F(SpaceMapTests, test_sm_recursive)
{
	do_tests<sm_recursive_creator>();
}

TEST_F(SpaceMapTests, test_sm_disk)
{
	do_tests<sm_disk_creator>();
	test_sm_reopen<sm_disk_creator>();
}

TEST_F(SpaceMapTests, test_sm_metadata)
{
	do_tests<sm_metadata_creator>();
	test_sm_reopen<sm_metadata_creator>();
}

TEST_F(SpaceMapTests, test_metadata_and_disk)
{
	block_manager<>::ptr bm(
		new block_manager<>("./test.data", NR_BLOCKS, MAX_LOCKS, block_manager<>::READ_WRITE));
	space_map::ptr core_sm(new core_map(NR_BLOCKS));
	transaction_manager::ptr tm(new transaction_manager(bm, core_sm));
	persistent_space_map::ptr metadata_sm = persistent_data::create_metadata_sm(*tm, NR_BLOCKS);
	copy_space_maps(metadata_sm, core_sm);
	tm->set_sm(metadata_sm);

	persistent_space_map::ptr data_sm_ = create_disk_sm(*tm, NR_BLOCKS * 2);
}

//----------------------------------------------------------------

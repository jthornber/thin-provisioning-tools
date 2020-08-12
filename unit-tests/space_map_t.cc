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
#include "persistent-data/space-maps/disk_structures.h"
#include "persistent-data/space-maps/recursive.h"
#include "test_utils.h"

using namespace std;
using namespace persistent_data;
using namespace testing;

//----------------------------------------------------------------

namespace {
	unsigned const ENTRIES_PER_BLOCK = (MD_BLOCK_SIZE - sizeof(sm_disk_detail::bitmap_header)) * 4;
	unsigned const NR_BITMAPS = 2;
	unsigned const NR_BLOCKS = ENTRIES_PER_BLOCK * NR_BITMAPS;
	block_address const SUPERBLOCK = 0;
	unsigned const MAX_LOCKS = 8;

	class SpaceMapTests : public Test {
	public:
		SpaceMapTests()
			: bm_(test::create_bm(NR_BLOCKS)),
			  sm_(create_core_map(NR_BLOCKS)),
			  tm_(bm_, sm_) {
		}

		struct sm_core_creator {
			static space_map::ptr
			create(transaction_manager &tm) {
				return create_core_map(NR_BLOCKS);
			}
		};

		struct sm_careful_alloc_creator {
			static space_map::ptr
			create(transaction_manager &tm) {
				return create_careful_alloc_sm(
					create_core_map(NR_BLOCKS));
			}
		};

		struct sm_recursive_creator {
			static checked_space_map::ptr
			create(transaction_manager &tm) {
				return create_recursive_sm(
					create_core_map(NR_BLOCKS));
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
				ASSERT_TRUE(!!mb);
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
			ASSERT_TRUE(!!mb);

			for (;;) {
				boost::optional<block_address> b = sm->new_block();
				if (!b)
					break;

				if (b) {
					ASSERT_TRUE(*b != *mb);
				}
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


		block_manager::ptr bm_;
		space_map::ptr sm_;
		transaction_manager tm_;
	};

	//--------------------------------

	class MetadataSpaceMapTests : public Test {
	public:
		MetadataSpaceMapTests() {
			memset(metadata_sm_root_, 0, sizeof(metadata_sm_root_));
			create_metadata_sm(NR_BLOCKS);
		}

		void commit() {
			metadata_sm_->commit();

			ASSERT_THAT(metadata_sm_->root_size(), Le(sizeof(metadata_sm_root_)));
			metadata_sm_->copy_root(metadata_sm_root_, sizeof(metadata_sm_root_));
		}

		void open() {
			bm_ = block_manager::ptr(new block_manager("./test.data", NR_BLOCKS, MAX_LOCKS, block_manager::READ_WRITE));
			space_map::ptr core_sm{create_core_map(NR_BLOCKS)};
			tm_ = transaction_manager::ptr(new transaction_manager(bm_, core_sm));
			metadata_sm_ = persistent_data::open_metadata_sm(*tm_, metadata_sm_root_);
			tm_->set_sm(metadata_sm_);
		}

		void create_data_sm(block_address nr_blocks) {
			data_sm_ = create_disk_sm(*tm_, nr_blocks);
		}

		void load_ies(std::vector<sm_disk_detail::index_entry> &entries) {
			sm_disk_detail::sm_root v;
			get_root(v);

			block_address nr_indexes = (v.nr_blocks_ + ENTRIES_PER_BLOCK - 1) / ENTRIES_PER_BLOCK;
			entries.resize(nr_indexes);
			ASSERT_EQ(entries.size(), NR_BITMAPS);

			block_manager::read_ref rr =
				bm_->read_lock(v.bitmap_root_, index_validator());
			sm_disk_detail::metadata_index const *mdi = reinterpret_cast<sm_disk_detail::metadata_index const *>(rr.data());
			for (block_address i = 0; i < nr_indexes; i++) {
				sm_disk_detail::index_entry_traits::unpack(*(mdi->index + i), entries[i]);
				ASSERT_EQ(metadata_sm_->get_count(entries[i].blocknr_), 1u);
			}
		}

		// note that the index_block is not shadowed until space map commit
		void get_root(sm_disk_detail::sm_root &v) const {
			sm_disk_detail::sm_root_disk d;
			metadata_sm_->copy_root(&d, sizeof(d));
			sm_disk_detail::sm_root_traits::unpack(d, v);
		}

		void mark_shadowed() {
			block_counter bc(true);
			metadata_sm_->count_metadata(bc);

			block_address nr_blocks = metadata_sm_->get_nr_blocks();
			for (block_address b = 0; b < nr_blocks; b++) {
				if (bc.get_count(b))
					tm_->mark_shadowed(b);
			}
		}

		checked_space_map::ptr metadata_sm_;
		checked_space_map::ptr data_sm_;

	private:
		void create_metadata_sm(block_address nr_blocks) {
			bm_ = test::create_bm(NR_BLOCKS);
			space_map::ptr core_sm{create_core_map(nr_blocks)};
			tm_ = transaction_manager::ptr(new transaction_manager(bm_, core_sm));
			metadata_sm_ = persistent_data::create_metadata_sm(*tm_, nr_blocks);
			copy_space_maps(metadata_sm_, core_sm);
			tm_->set_sm(metadata_sm_);
		}

		void copy_space_maps(space_map::ptr lhs, space_map::ptr rhs) {
			for (block_address b = 0; b < rhs->get_nr_blocks(); b++) {
				uint32_t count = rhs->get_count(b);
				if (count > 0)
					lhs->set_count(b, rhs->get_count(b));
			}
		}

		block_manager::ptr bm_;
		transaction_manager::ptr tm_;

		unsigned char metadata_sm_root_[128];
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

//----------------------------------------------------------------

TEST_F(MetadataSpaceMapTests, test_metadata_and_disk)
{
	create_data_sm(NR_BLOCKS * 2);
}

// Test whether sm_recursive protected allocated blocks in a recursion
// (github issue #70)
TEST_F(MetadataSpaceMapTests, test_allocate_blocks)
{
	commit();
	open();

	std::vector<sm_disk_detail::index_entry> entries;
	load_ies(entries);

	block_address nr_allocatable = metadata_sm_->get_nr_free() - NR_BITMAPS;
	block_address nr_allocated = 0;
	while (true) {
		space_map::maybe_block b = metadata_sm_->new_block();
		if (!b)
			break;
		++nr_allocated;

		for (auto const &e : entries)
			ASSERT_NE(*b, e.blocknr_);
	}

	ASSERT_EQ(nr_allocated, nr_allocatable);
	for (auto const &e : entries)
		ASSERT_EQ(metadata_sm_->get_count(e.blocknr_), 0u);
}

// Test whether sm_careful_alloc protects released blocks in a recursion.
// (github issue #97)
TEST_F(MetadataSpaceMapTests, test_multiple_shadows_by_dec)
{
	// Occupy all the blocks belonging to the first bitmap,
	// to change the position of further allocated blocks.
	for (unsigned i = 0; i < ENTRIES_PER_BLOCK; i++)
		metadata_sm_->new_block();

	// the extra two blocks are the index block and the ref-count tree root
	ASSERT_EQ(metadata_sm_->get_nr_free(), ENTRIES_PER_BLOCK - NR_BITMAPS - 2);

	commit();
	open();

	std::vector<sm_disk_detail::index_entry> entries;
	load_ies(entries);

	// Releasing the block belonging to the second bitmap results in
	// shadowing the second, then the first bitmap blocks.
	// The shadow operations must not reuse the released block.
	block_address last = metadata_sm_->get_nr_blocks() - metadata_sm_->get_nr_free() - 1;
	ASSERT_EQ(metadata_sm_->get_count(last), 1u);
	metadata_sm_->dec(last);

	// assert that the released blocks are not used
	ASSERT_EQ(metadata_sm_->get_count(last), 0u);
	for (auto const &e : entries)
		ASSERT_EQ(metadata_sm_->get_count(e.blocknr_), 0u);

	ASSERT_EQ(metadata_sm_->get_nr_free(), ENTRIES_PER_BLOCK - NR_BITMAPS - 1);

	// check ref-counts of shadowed bitmaps
	commit();
	open();
	load_ies(entries);
}

// Test whether sm_recursive protects allocated blocks in a recursion
// (github issue #70)
TEST_F(MetadataSpaceMapTests, test_multiple_shadows_by_inc)
{
	// Occupy all the blocks belonging to the first bitmap,
	// to change the position of further allocated blocks.
	for (unsigned i = 0; i < ENTRIES_PER_BLOCK; i++)
		metadata_sm_->new_block();

	// the extra two blocks are the index block and the ref-count tree root
	ASSERT_EQ(metadata_sm_->get_nr_free(), ENTRIES_PER_BLOCK - NR_BITMAPS - 2);

	commit();
	open();

	std::vector<sm_disk_detail::index_entry> entries;
	load_ies(entries);

	// allocating a block results in shadowing the second,
	// then the first bitmap
	space_map::maybe_block b = metadata_sm_->new_block();

	// assert reference counts
	ASSERT_EQ(metadata_sm_->get_count(*b), 1u);
	for (auto const &e : entries)
		ASSERT_EQ(metadata_sm_->get_count(e.blocknr_), 0u);

	ASSERT_EQ(metadata_sm_->get_nr_free(), ENTRIES_PER_BLOCK - NR_BITMAPS - 3);

	// check ref-counts of shadowed bitmaps
	commit();
	open();
	load_ies(entries);
}

// Test whether intended in-place modification works
TEST_F(MetadataSpaceMapTests, test_intended_in_place_modification)
{
	sm_disk_detail::sm_root root;
	get_root(root);
	std::vector<sm_disk_detail::index_entry> entries;
	load_ies(entries);

	commit();
	open();
	mark_shadowed();

	// the bitmaps won't be shadowed,
	// all the free blocks therefore become allocatable
	block_address nr_allocatable = metadata_sm_->get_nr_free();
	block_address nr_allocated = 0;
	while (true) {
		space_map::maybe_block b = metadata_sm_->new_block();
		if (!b)
			break;
		++nr_allocated;

		for (auto const &e : entries)
			ASSERT_NE(*b, e.blocknr_);
	}

	ASSERT_EQ(nr_allocated, nr_allocatable);

	commit();
	open();

	sm_disk_detail::sm_root root2;
	get_root(root2);
	std::vector<sm_disk_detail::index_entry> entries2;
	load_ies(entries2);

	// assert the space map block locations are not changed
	ASSERT_EQ(root.bitmap_root_, root2.bitmap_root_);
	ASSERT_EQ(root.ref_count_root_, root2.ref_count_root_);
	ASSERT_EQ(entries.size(), entries2.size());
	for (unsigned i = 0; i < entries.size(); i++)
		ASSERT_EQ(entries[i].blocknr_, entries2[i].blocknr_);

	ASSERT_EQ(root2.nr_allocated_, root.nr_allocated_ + nr_allocated);
}

//----------------------------------------------------------------

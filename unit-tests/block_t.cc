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
#include "persistent-data/block.h"
#include "test_utils.h"
#include <stdlib.h>

using namespace std;
using namespace test;
using namespace testing;

//----------------------------------------------------------------

namespace {
	template <uint32_t BlockSize>
	void check_all_bytes(typename block_manager<BlockSize>::read_ref const &rr, int v) {
		persistent_data::buffer<BlockSize> const &data = rr.data();
		for (unsigned b = 0; b < BlockSize; b++)
			ASSERT_THAT(data[b], Eq(static_cast<unsigned char>(v)));
	}

	template <uint32_t BlockSize>
	struct zero_validator : public block_manager<BlockSize>::validator {
		virtual void check(buffer<BlockSize> const &data, block_address location) const {
			for (unsigned b = 0; b < BlockSize; b++)
				if (data[b] != 0)
 					throw runtime_error("validator check zero");
		}

		virtual void prepare(buffer<BlockSize> &data, block_address location) const {
			for (unsigned b = 0; b < BlockSize; b++)
				data[b] = 0;
		}
	};

	typedef block_manager<4096> bm4096;
}

//----------------------------------------------------------------

TEST(BlockTests, bad_path)
{
	ASSERT_THROW(bm4096("/bogus/bogus/bogus", 1234, 4, block_io<>::READ_WRITE),
			  runtime_error);
}

TEST(BlockTests, out_of_range_access)
{
	bm4096::ptr bm = create_bm<4096>(1024);
	ASSERT_THROW(bm->read_lock(1024), runtime_error);
}

TEST(BlockTests, read_lock_all_blocks)
{
	block_address const nr = 64;
	bm4096::ptr bm = create_bm<4096>(nr);
	for (unsigned i = 0; i < nr; i++)
		bm->read_lock(i);
}

TEST(BlockTests, write_lock_all_blocks)
{
	block_address const nr = 64;
	bm4096::ptr bm = create_bm<4096>(nr);
	for (unsigned i = 0; i < nr; i++)
		bm->write_lock(i);
}

TEST(BlockTests, writes_persist)
{
	block_address const nr = 64;
	bm4096::ptr bm = create_bm<4096>(nr);
	for (unsigned i = 0; i < nr; i++) {
		bm4096::write_ref wr = bm->write_lock(i);
		::memset(wr.data().raw(), i, 4096);
	}

	for (unsigned i = 0; i < nr; i++) {
		bm4096::read_ref rr = bm->read_lock(i);
		check_all_bytes<4096>(rr, i % 256);
	}
}

TEST(BlockTests, write_lock_zero_zeroes)
{
	bm4096::ptr bm = create_bm<4096>(64);
	check_all_bytes<4096>(bm->write_lock_zero(23), 0);
}

TEST(BlockTests, different_block_sizes)
{
	{
		bm4096::ptr bm = create_bm<4096>(64);
		bm4096::read_ref rr = bm->read_lock(0);
		ASSERT_THAT(sizeof(rr.data()), Eq(4096u));
	}

	{
		block_manager<64 * 1024>::ptr bm = create_bm<64 * 1024>(64);
		block_manager<64 * 1024>::read_ref rr = bm->read_lock(0);
		ASSERT_THAT(sizeof(rr.data()), Eq(64u * 1024u));
	}
}

TEST(BlockTests, read_validator_works)
{
	bm4096::block_manager::validator::ptr v(new zero_validator<4096>());
	bm4096::ptr bm = create_bm<4096>(64);
	bm->write_lock_zero(0);
	bm->read_lock(0, v);
}

TEST(BlockTests, write_validator_works)
{
	bm4096::ptr bm = create_bm<4096>(64);
	bm4096::block_manager::validator::ptr v(new zero_validator<4096>());

	{
		bm4096::write_ref wr = bm->write_lock(0, v);
		::memset(wr.data().raw(), 23, sizeof(wr.data().raw()));
	}

	bm->flush();		// force the prepare method to be called
	check_all_bytes<4096>(bm->read_lock(0), 0);
}

TEST(BlockTests, cannot_have_two_superblocks)
{
	bm4096::ptr bm = create_bm<4096>();
	bm4096::write_ref superblock = bm->superblock(0);
	ASSERT_THROW(bm->superblock(1), runtime_error);
}

TEST(BlockTests, can_have_subsequent_superblocks)
{
	bm4096::ptr bm = create_bm<4096>();
	{ bm4096::write_ref superblock = bm->superblock(0); }
	{ bm4096::write_ref superblock = bm->superblock(0); }
}

TEST(BlockTests, superblocks_can_change_address)
{
	bm4096::ptr bm = create_bm<4096>();
	{ bm4096::write_ref superblock = bm->superblock(0); }
	{ bm4096::write_ref superblock = bm->superblock(1); }
}

TEST(BlockTests, superblock_must_be_last)
{
	bm4096::ptr bm = create_bm<4096>();
	{
		bm4096::read_ref rr = bm->read_lock(63);
		{
			ASSERT_THROW(bm->superblock(0), runtime_error);
		}
	}
}

TEST(BlockTests, references_can_be_copied)
{
	bm4096::ptr bm = create_bm<4096>();
	bm4096::write_ref wr1 = bm->write_lock(0);
	bm4096::write_ref wr2(wr1);
}

TEST(BlockTests, no_concurrent_write_locks)
{
	bm4096::ptr bm = create_bm<4096>();
	bm4096::write_ref wr = bm->write_lock(0);
	ASSERT_THROW(bm->write_lock(0), runtime_error);
}

TEST(BlockTests, concurrent_read_locks)
{
	bm4096::ptr bm = create_bm<4096>();
	bm4096::read_ref rr = bm->read_lock(0);
	bm->read_lock(0);
}

TEST(BlockTests, read_then_write)
{
	bm4096::ptr bm = create_bm<4096>();
	bm->read_lock(0);
	bm->write_lock(0);
}

TEST(BlockTests, write_then_read)
{
	bm4096::ptr bm = create_bm<4096>();
	bm->write_lock(0);
	bm->read_lock(0);
}

//----------------------------------------------------------------

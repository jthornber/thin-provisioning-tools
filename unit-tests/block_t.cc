// Copyright (C) 20011 Red Hat, Inc. All rights reserved.
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

#include "block.h"

#define BOOST_TEST_MODULE BlockManagerTests
#include <boost/test/included/unit_test.hpp>

using namespace std;

//----------------------------------------------------------------

namespace {
	unsigned const MAX_HELD_LOCKS = 16;

	block_manager<4096>::ptr create_bm(block_address nr = 1024) {
		return block_manager<4096>::ptr(new block_manager<4096>("./test.data", nr, MAX_HELD_LOCKS, true));
	}

	template <uint32_t BlockSize>
	void check_all_bytes(typename block_manager<BlockSize>::read_ref const &rr, int v) {
		typename block_manager<BlockSize>::const_buffer &data = rr.data();
		for (unsigned b = 0; b < BlockSize; b++)
			BOOST_CHECK_EQUAL(data[b], v);
	}

	template <uint32_t BlockSize>
	class zero_validator : public block_manager<BlockSize>::validator {
		void check(block_manager<4096>::const_buffer &data, block_address location) const {
			for (unsigned b = 0; b < BlockSize; b++)
				if (data[b] != 0)
 					throw runtime_error("validator check zero");
		}

		void prepare(block_manager<4096>::buffer &data, block_address location) const {
			cerr << "zeroing" << endl;
			for (unsigned b = 0; b < BlockSize; b++)
				data[b] = 0;
		}
	};

	typedef block_manager<4096> bm4096;
}

//----------------------------------------------------------------

BOOST_AUTO_TEST_CASE(bad_path)
{
	BOOST_CHECK_THROW(bm4096("/bogus/bogus/bogus", 1234, 4), runtime_error);
}

BOOST_AUTO_TEST_CASE(out_of_range_access)
{
	bm4096::ptr bm = create_bm(1024);
	BOOST_CHECK_THROW(bm->read_lock(1024), runtime_error);
}

BOOST_AUTO_TEST_CASE(read_lock_all_blocks)
{
	block_address const nr = 64;
	bm4096::ptr bm = create_bm(nr);
	for (unsigned i = 0; i < nr; i++)
		bm->read_lock(i);
}

BOOST_AUTO_TEST_CASE(write_lock_all_blocks)
{
	block_address const nr = 64;
	bm4096::ptr bm = create_bm(nr);
	for (unsigned i = 0; i < nr; i++)
		bm->write_lock(i);
}

BOOST_AUTO_TEST_CASE(writes_persist)
{
	block_address const nr = 64;
	bm4096::ptr bm = create_bm(nr);
	for (unsigned i = 0; i < nr; i++) {
		bm4096::write_ref wr = bm->write_lock(i);
		::memset(wr.data(), i, 4096);
	}

	for (unsigned i = 0; i < nr; i++) {
		bm4096::read_ref rr = bm->read_lock(i);
		check_all_bytes<4096>(rr, i % 256);
	}
}

BOOST_AUTO_TEST_CASE(write_lock_zero_zeroes)
{
	bm4096::ptr bm = create_bm(64);
	check_all_bytes<4096>(bm->write_lock_zero(23), 0);
}

BOOST_AUTO_TEST_CASE(different_block_sizes)
{
	{
		bm4096 bm("./test.data", 64, 1);
		bm4096::read_ref rr = bm.read_lock(0);
		BOOST_CHECK_EQUAL(sizeof(rr.data()), 4096);
	}

	{
		block_manager<64 * 1024> bm("./test.data", 64, true);
		block_manager<64 * 1024>::read_ref rr = bm.read_lock(0);
		BOOST_CHECK_EQUAL(sizeof(rr.data()), 64 * 1024);
	}
}

BOOST_AUTO_TEST_CASE(read_validator_works)
{
	bm4096::block_manager::validator::ptr v(new zero_validator<4096>());
	bm4096::ptr bm = create_bm(64);
	bm->write_lock_zero(0);
	bm->read_lock(0, v);
}

BOOST_AUTO_TEST_CASE(write_validator_works)
{
	bm4096::ptr bm = create_bm(64);
	bm4096::block_manager::validator::ptr v(new zero_validator<4096>());

	{
		bm4096::write_ref wr = bm->write_lock(0, v);
		::memset(wr.data(), 23, sizeof(wr.data()));
	}

	bm->flush();		// force the prepare method to be called
	check_all_bytes<4096>(bm->read_lock(0), 0);
}

BOOST_AUTO_TEST_CASE(cannot_have_two_superblocks)
{
	bm4096::ptr bm = create_bm();
	bm4096::write_ref superblock = bm->superblock(0);
	BOOST_CHECK_THROW(bm->superblock(1), runtime_error);
}

BOOST_AUTO_TEST_CASE(can_have_subsequent_superblocks)
{
	bm4096::ptr bm = create_bm();
	{ bm4096::write_ref superblock = bm->superblock(0); }
	{ bm4096::write_ref superblock = bm->superblock(0); }
}

BOOST_AUTO_TEST_CASE(superblocks_can_change_address)
{
	bm4096::ptr bm = create_bm();
	{ bm4096::write_ref superblock = bm->superblock(0); }
	{ bm4096::write_ref superblock = bm->superblock(1); }
}

BOOST_AUTO_TEST_CASE(superblock_must_be_last)
{
	bm4096::ptr bm = create_bm();
	{
		bm4096::read_ref rr = bm->read_lock(63);
		{
			BOOST_CHECK_THROW(bm->superblock(0), runtime_error);
		}
	}
}

BOOST_AUTO_TEST_CASE(references_can_be_copied)
{
	bm4096::ptr bm = create_bm();
	bm4096::write_ref wr1 = bm->write_lock(0);
	bm4096::write_ref wr2(wr1);
}

#if 0
BOOST_AUTO_TEST_CASE(flush_throws_if_held_locks)
{
	bm4096::ptr bm = create_bm();
	bm4096::write_ref wr = bm->write_lock(0);
	BOOST_CHECK_THROW(bm->flush(), runtime_error);
}
#endif

BOOST_AUTO_TEST_CASE(no_concurrent_write_locks)
{
	bm4096::ptr bm = create_bm();
	bm4096::write_ref wr = bm->write_lock(0);
	BOOST_CHECK_THROW(bm->write_lock(0), runtime_error);
}

BOOST_AUTO_TEST_CASE(concurrent_read_locks)
{
	bm4096::ptr bm = create_bm();
	bm4096::read_ref rr = bm->read_lock(0);
	bm->read_lock(0);
}

BOOST_AUTO_TEST_CASE(read_then_write)
{
	bm4096::ptr bm = create_bm();
	bm->read_lock(0);
	bm->write_lock(0);
}

BOOST_AUTO_TEST_CASE(write_then_read)
{
	bm4096::ptr bm = create_bm();
	bm->write_lock(0);
	bm->read_lock(0);
}

//----------------------------------------------------------------

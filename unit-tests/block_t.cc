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
		unsigned char const *data = reinterpret_cast<unsigned char const *>(rr.data());
		for (unsigned b = 0; b < BlockSize; b++)
			ASSERT_THAT(data[b], Eq(static_cast<unsigned char>(v)));
	}

	template <uint32_t BlockSize>
	struct zero_validator : public bcache::validator {
		virtual void check(void const *raw, block_address location) const {
			unsigned char const *data = reinterpret_cast<unsigned char const *>(raw);
			for (unsigned b = 0; b < BlockSize; b++)
				if (data[b] != 0)
 					throw runtime_error("validator check zero");
		}

		virtual bool check_raw(void const *raw) const {
			unsigned char const *data = reinterpret_cast<unsigned char const *>(raw);
			for (unsigned b = 0; b < BlockSize; b++)
				if (data[b] != 0)
					return false;
			return true;
		}

		virtual void prepare(void *raw, block_address location) const {
			unsigned char *data = reinterpret_cast<unsigned char *>(raw);
			for (unsigned b = 0; b < BlockSize; b++)
				data[b] = 0;
		}
	};

	class validator_mock : public bcache::validator {
	public:
		typedef boost::shared_ptr<validator_mock> ptr;

		MOCK_CONST_METHOD2(check, void(void const *, block_address));
		MOCK_CONST_METHOD2(prepare, void(void *, block_address));
	};

	typedef block_manager<4096> bm4096;
}

//----------------------------------------------------------------

TEST(BlockTests, bad_path)
{
	ASSERT_THROW(bm4096("/bogus/bogus/bogus", 1234, 4, block_manager<>::READ_WRITE),
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
		::memset(wr.data(), i, 4096);
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

		{
			bm4096::write_ref wr = bm->write_lock(0);
			memset(wr.data(), 23, 4096);
		}

		{
			bm4096::write_ref wr = bm->write_lock_zero(0);
			check_all_bytes<4096>(wr, 0);
		}
	}

	{
		block_manager<64 * 1024>::ptr bm = create_bm<64 * 1024>(64);

		{
			block_manager<64 * 1024>::write_ref wr = bm->write_lock(0);
			memset(wr.data(), 72, 64 * 1024);
		}

		{
			block_manager<64 * 1024>::write_ref wr = bm->write_lock_zero(0);
			check_all_bytes<64 * 1024>(wr, 0);
		}
	}
}

TEST(BlockTests, read_validator_works)
{
	bcache::validator::ptr v(new zero_validator<4096>());
	bm4096::ptr bm = create_bm<4096>(64);
	bm->write_lock_zero(0);
	bm->read_lock(0, v);
}

TEST(BlockTests, write_validator_works)
{
	bm4096::ptr bm = create_bm<4096>(64);
	bcache::validator::ptr v(new zero_validator<4096>());

	{
		bm4096::write_ref wr = bm->write_lock(0, v);
		::memset(wr.data(), 23, 4096);
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

// This test terminates with g++ 4.8.1.  This is because it erroneously
// assumes a noexcept(true) specification is present on every declared
// destructor.  Not a bad idea, but not what the standard says.  Obviously
// we really shouldn't be throwing in a destructor, but not sure what the
// correct thing to do is (log the error? put the tm into a 'bad' state?).
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

namespace {
	class ValidatorTests : public Test {
	public:
		ValidatorTests()
			: bm(create_bm<4096>()),
			  vmock(new validator_mock),
			  vmock2(new validator_mock) {
		}

		void expect_check(validator_mock::ptr v) {
			EXPECT_CALL(*v, check(_, Eq(0ull))).Times(1);
		}

		void expect_no_check(validator_mock::ptr v) {
			EXPECT_CALL(*v, check(_, Eq(0ull))).Times(0);
		}

		void expect_prepare(validator_mock::ptr v) {
			EXPECT_CALL(*v, prepare(_, Eq(0ull))).Times(1);
		}

		bm4096::ptr bm;
		validator_mock::ptr vmock;
		validator_mock::ptr vmock2;
	};

	class my_error : public runtime_error {
	public:
		my_error(string const &msg)
		: runtime_error(msg) {
		}
	};
}

//--------------------------------

TEST_F(ValidatorTests, check_on_read_lock)
{
	expect_check(vmock);
	bm4096::read_ref rr = bm->read_lock(0, vmock);
}

TEST_F(ValidatorTests, check_only_called_once_on_read_lock)
{
	{
		expect_check(vmock);
		bm4096::read_ref rr = bm->read_lock(0, vmock);
	}

	expect_no_check(vmock);
	bm4096::read_ref rr = bm->read_lock(0, vmock);
}

TEST_F(ValidatorTests, validator_can_be_changed_by_read_lock)
{
	{
		expect_check(vmock);
		bm4096::read_ref rr = bm->read_lock(0, vmock);
	}

	{
		expect_no_check(vmock);
		expect_check(vmock2);
		bm4096::read_ref rr = bm->read_lock(0, vmock2);
	}
}

//--------------------------------

TEST_F(ValidatorTests, check_and_prepare_on_write_lock)
{
	expect_check(vmock);
	expect_prepare(vmock);
	bm4096::write_ref wr = bm->write_lock(0, vmock);
}

TEST_F(ValidatorTests, check_only_called_once_on_write_lock)
{
	{
		expect_check(vmock);
		bm4096::write_ref wr = bm->write_lock(0, vmock);
	}

	expect_no_check(vmock);
	expect_prepare(vmock);

	bm4096::write_ref wr = bm->write_lock(0, vmock);
}

TEST_F(ValidatorTests, validator_can_be_changed_by_write_lock)
{
	{
		expect_check(vmock);
		expect_prepare(vmock);
		bm4096::write_ref wr = bm->write_lock(0, vmock);
	}

	{
		expect_check(vmock2);
		expect_prepare(vmock2);
		bm4096::write_ref wr = bm->write_lock(0, vmock2);
	}
}

//--------------------------------

TEST_F(ValidatorTests, no_check_but_prepare_on_write_lock_zero)
{
	expect_no_check(vmock);
	expect_prepare(vmock);
	bm4096::write_ref wr = bm->write_lock_zero(0, vmock);
}

TEST_F(ValidatorTests, validator_can_be_changed_by_write_lock_zero)
{
	{
		expect_no_check(vmock);
		expect_prepare(vmock);
		bm4096::write_ref wr = bm->write_lock_zero(0, vmock);
	}
	// We need to flush to ensure the vmock->prepare has occurred
	bm->flush();

	expect_no_check(vmock2);
	expect_prepare(vmock2);
	bm4096::write_ref wr = bm->write_lock_zero(0, vmock2);
}

//--------------------------------

TEST_F(ValidatorTests, check_and_prepare_on_superblock_lock)
{
	expect_check(vmock);
	expect_prepare(vmock);
	bm4096::write_ref wr = bm->superblock(0, vmock);
}

TEST_F(ValidatorTests, check_only_called_once_on_superblock_lock)
{
	{
		expect_check(vmock);
		expect_prepare(vmock);
		bm4096::write_ref wr = bm->superblock(0, vmock);
	}

	expect_no_check(vmock);
	expect_prepare(vmock);

	bm4096::write_ref wr = bm->superblock(0, vmock);
}

TEST_F(ValidatorTests, validator_can_be_changed_by_superblock_lock)
{
	{
		expect_check(vmock);
		expect_prepare(vmock);

		bm4096::write_ref wr = bm->write_lock(0, vmock);
	}

	{
		expect_no_check(vmock);
		expect_check(vmock2);
		expect_prepare(vmock2);

		bm4096::write_ref wr = bm->write_lock(0, vmock2);
	}
}

//--------------------------------

TEST_F(ValidatorTests, no_check_but_prepare_on_superblock_lock_zero)
{
	expect_no_check(vmock);
	expect_prepare(vmock);

	bm4096::write_ref wr = bm->superblock_zero(0, vmock);
}

TEST_F(ValidatorTests, validator_can_be_changed_by_superblock_zero)
{
	{
		expect_no_check(vmock);
		expect_prepare(vmock);
		bm4096::write_ref wr = bm->superblock_zero(0, vmock);
	}

	expect_no_check(vmock2);
	expect_prepare(vmock2);

	bm4096::write_ref wr = bm->superblock_zero(0, vmock2);
}

//--------------------------------

TEST_F(ValidatorTests, validator_check_failure_gets_passed_up)
{
	validator_mock::ptr v(new validator_mock);

	EXPECT_CALL(*v, check(_, Eq(0ull))).Times(1).WillOnce(Throw(my_error("bang!")));

	ASSERT_THROW(bm->read_lock(0, v), my_error);
	// FIXME: put this back in
	//ASSERT_FALSE(bm->is_locked(0));
}

//----------------------------------------------------------------

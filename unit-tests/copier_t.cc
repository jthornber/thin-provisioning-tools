// Copyright (C) 2016 Red Hat, Inc. All rights reserved.
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
#include "block-cache/copier.h"
#include "test_utils.h"

#include <fcntl.h>

using namespace boost;
using namespace std;
using namespace test;
using namespace testing;

//----------------------------------------------------------------

namespace {
	unsigned const BLOCK_SIZE = 64u;
	using wait_result = io_engine::wait_result;

	ostream &operator <<(ostream &out, optional<wait_result> const &mwr) {
		if (mwr) {
			out << "Just[wait_result[" << mwr->first << ", " << mwr->second << "]]";
		} else
			out << "Nothing";
		return out;
	}

	class io_engine_mock : public io_engine {
	public:
		MOCK_METHOD3(open_file, handle(string const &, mode, sharing));
		MOCK_METHOD1(close_file, void(handle));
		MOCK_METHOD6(issue_io, bool(handle, dir, sector_t, sector_t, void *, unsigned));

		MOCK_METHOD0(wait, optional<wait_result>());
		MOCK_METHOD1(wait, optional<wait_result>(unsigned &));
	};

	class CopierTests : public Test {
	public:
		CopierTests()
			: src_file_("copy_src"),
			  dest_file_("copy_dest") {
		}

		unique_ptr<copier> make_copier() {
			return make_copier(BLOCK_SIZE, 0, 0);
		}

		unique_ptr<copier> make_copier(sector_t block_size,
					       sector_t src_offset,
					       sector_t dest_offset) {
			EXPECT_CALL(engine_, open_file(src_file_, io_engine::M_READ_ONLY, io_engine::EXCLUSIVE)).
				WillOnce(Return(SRC_HANDLE));
			EXPECT_CALL(engine_, open_file(dest_file_, io_engine::M_READ_WRITE, io_engine::EXCLUSIVE)).
				WillOnce(Return(DEST_HANDLE));

			EXPECT_CALL(engine_, close_file(SRC_HANDLE)).Times(1);
			EXPECT_CALL(engine_, close_file(DEST_HANDLE)).Times(1);

			return unique_ptr<copier>(new copier(engine_, src_file_,
							     dest_file_,
							     block_size, 1 * 1024 * 1024,
							     src_offset, dest_offset));
		}

		static optional<wait_result> make_wr(bool success, unsigned context) {
			return optional<wait_result>(wait_result(success, context));
		}

		// issue a copy_op, and wait for its completion synchronously
		void issue_successful_op(copier &c, copy_op &op, unsigned context) {
			InSequence dummy;

			unsigned nr_pending = c.nr_pending();
			EXPECT_CALL(engine_, issue_io(SRC_HANDLE, io_engine::D_READ,
						      expected_src_sector(c, op.src_b),
						      expected_src_sector(c, op.src_e), _, context)).
				WillOnce(Return(true));
			c.issue(op);

			ASSERT_TRUE(c.nr_pending() == nr_pending + 1);

			EXPECT_CALL(engine_, wait()).
				WillOnce(Return(make_wr(true, context)));

			EXPECT_CALL(engine_, issue_io(DEST_HANDLE, io_engine::D_WRITE,
						      expected_dest_sector(c, op.dest_b),
						      expected_dest_sector(c, op.dest_b + (op.src_e - op.src_b)),
						      _, context)).
				WillOnce(Return(true));

			EXPECT_CALL(engine_, wait()).
				WillOnce(Return(make_wr(true, context)));

			auto mop = c.wait();
			ASSERT_EQ(c.nr_pending(), nr_pending);

			ASSERT_TRUE(mop->success());
		}

		unsigned const SRC_HANDLE = 10;
		unsigned const DEST_HANDLE = 11;

		StrictMock<io_engine_mock> engine_;

	private:
		sector_t expected_src_sector(const copier &c, block_address b) {
			return c.get_src_offset() + b * c.get_block_size();
		}

		sector_t expected_dest_sector(const copier &c, block_address b) {
			return c.get_dest_offset() + b * c.get_block_size();
		}

		string src_file_;
		string dest_file_;
	};
}

//----------------------------------------------------------------

TEST_F(CopierTests, create_default_copier)
{
	auto c = make_copier();
	ASSERT_EQ(c->get_block_size(), BLOCK_SIZE);
	ASSERT_EQ(c->get_src_offset(), 0u);
	ASSERT_EQ(c->get_dest_offset(), 0u);
}

TEST_F(CopierTests, create_copier_with_offsets)
{
	sector_t src_offset = 2048;
	sector_t dest_offset = 8192;

	auto c = make_copier(BLOCK_SIZE, src_offset, dest_offset);
	ASSERT_EQ(c->get_block_size(), BLOCK_SIZE);
	ASSERT_EQ(c->get_src_offset(), src_offset);
	ASSERT_EQ(c->get_dest_offset(), dest_offset);
}

TEST_F(CopierTests, successful_copy)
{
	// Copy first block
	copy_op op1(0, 1, 0);

	auto c = make_copier();
	issue_successful_op(*c, op1, 0);
}

TEST_F(CopierTests, successful_copy_with_offsets)
{
	copy_op op1(0, 1, 0);

	auto c = make_copier(BLOCK_SIZE, 2048, 8192);
	issue_successful_op(*c, op1, 0);
}

// Verify copier's error handling against unsucessful engine operations
// at different stages.
// Test the first stage (issue_read (failed) => read => issue_write => write)
TEST_F(CopierTests, unsuccessful_issue_read)
{
	copy_op op1(0, 1, 0);
	auto c = make_copier();

	InSequence dummy;
	EXPECT_CALL(engine_, issue_io(SRC_HANDLE, io_engine::D_READ, 0, BLOCK_SIZE, _, 0)).
		WillOnce(Return(false));
	c->issue(op1);

	ASSERT_EQ(c->nr_pending(), 1u);

	auto mop = c->wait();
	ASSERT_EQ(c->nr_pending(), 0u);
	ASSERT_FALSE(mop->success());
}

// Test the second stage (issue_read => read (failed) => issue_write => write)
TEST_F(CopierTests, unsuccessful_read)
{
	copy_op op1(0, 1, 0);
	auto c = make_copier();

	InSequence dummy;
	EXPECT_CALL(engine_, issue_io(SRC_HANDLE, io_engine::D_READ, 0, BLOCK_SIZE, _, 0)).
		WillOnce(Return(true));
	c->issue(op1);

	ASSERT_EQ(c->nr_pending(), 1u);
	EXPECT_CALL(engine_, wait()).
		WillOnce(Return(make_wr(false, 0u)));
	ASSERT_EQ(c->nr_pending(), 1u);

	auto mop = c->wait();
	ASSERT_EQ(c->nr_pending(), 0u);
	ASSERT_FALSE(mop->success());
}

// Test the third stage (issue_read => read => issue_write (failed) => write)
TEST_F(CopierTests, unsuccessful_issue_write)
{
	copy_op op1(0, 1, 0);
	auto c = make_copier();

	InSequence dummy;
	EXPECT_CALL(engine_, issue_io(SRC_HANDLE, io_engine::D_READ, 0, BLOCK_SIZE, _, 0)).
		WillOnce(Return(true));
	c->issue(op1);

	ASSERT_EQ(c->nr_pending(), 1u);

	EXPECT_CALL(engine_, wait()).
		WillOnce(Return(make_wr(true, 0u)));
	ASSERT_EQ(c->nr_pending(), 1u);

	EXPECT_CALL(engine_, issue_io(DEST_HANDLE, io_engine::D_WRITE, 0, BLOCK_SIZE, _, 0)).
		WillOnce(Return(false));

	auto mop = c->wait();
	ASSERT_EQ(c->nr_pending(), 0u);
	ASSERT_FALSE(mop->success());
}

// Test the last stage (issue_read => read => issue_write => write (failed))
TEST_F(CopierTests, unsuccessful_write)
{
	// Copy first block
	copy_op op1(0, 1, 0);

	auto c = make_copier();

	InSequence dummy;
	EXPECT_CALL(engine_, issue_io(SRC_HANDLE, io_engine::D_READ, 0, BLOCK_SIZE, _, 0)).
		WillOnce(Return(true));
	c->issue(op1);
	ASSERT_EQ(c->nr_pending(), 1u);

	EXPECT_CALL(engine_, wait()).
		WillOnce(Return(make_wr(true, 0u)));

	EXPECT_CALL(engine_, issue_io(DEST_HANDLE, io_engine::D_WRITE, 0, BLOCK_SIZE, _, 0)).
		WillOnce(Return(true));

	EXPECT_CALL(engine_, wait()).
		WillOnce(Return(make_wr(false, 0u)));

	auto mop = c->wait();
	ASSERT_EQ(c->nr_pending(), 0u);

	ASSERT_FALSE(mop->success());
}

TEST_F(CopierTests, copy_the_same_block_many_times)
{
	auto c = make_copier();
	copy_op op1(0, 1, 0);

	for (unsigned i = 0; i < 50000; i++)
		issue_successful_op(*c, op1, i);
}

TEST_F(CopierTests, copy_different_blocks)
{
	auto c = make_copier();
	for (unsigned i = 0; i < 5000; i++) {
		copy_op op(i, i + 1, i);
		issue_successful_op(*c, op, i);
	}
}

TEST_F(CopierTests, copy_different_blocks_with_offsets)
{
	sector_t src_offset = 2048;
	sector_t dest_offset = 8192;

	auto c = make_copier(BLOCK_SIZE, src_offset, dest_offset);
	for (unsigned i = 0; i < 5000; i++) {
		copy_op op(i, i + 1, i);
		issue_successful_op(*c, op, i);
	}
}

TEST_F(CopierTests, wait_can_timeout)
{
	copy_op op1(0, 1, 0);
	auto c = make_copier();

	InSequence dummy;
	EXPECT_CALL(engine_, issue_io(SRC_HANDLE, io_engine::D_READ, 0, BLOCK_SIZE, _, 0)).
		WillOnce(Return(true));
	c->issue(op1);

	ASSERT_EQ(c->nr_pending(), 1u);

	unsigned micro = 10000;
	EXPECT_CALL(engine_, wait(micro)).
		WillOnce(Return(make_wr(true, 0u)));
	ASSERT_EQ(c->nr_pending(), 1u);

	EXPECT_CALL(engine_, issue_io(DEST_HANDLE, io_engine::D_WRITE, 0, BLOCK_SIZE, _, 0)).
		WillOnce(Return(true));

	EXPECT_CALL(engine_, wait(micro)).
		WillOnce(DoAll(SetArgReferee<0>(0u), Return(optional<wait_result>())));

	auto mop = c->wait(micro);
	ASSERT_FALSE(mop);
	ASSERT_EQ(c->nr_pending(), 1u);
}

//----------------------------------------------------------------

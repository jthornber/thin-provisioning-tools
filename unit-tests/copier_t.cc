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

	ostream &operator <<(ostream &out, wait_result const &wr) {
		out << "wait_result[" << wr.first << ", " << wr.second << "]";
		return out;
	}

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
			EXPECT_CALL(engine_, open_file(src_file_, io_engine::M_READ_ONLY, io_engine::EXCLUSIVE)).
				WillOnce(Return(SRC_HANDLE));
			EXPECT_CALL(engine_, open_file(dest_file_, io_engine::M_READ_WRITE, io_engine::EXCLUSIVE)).
				WillOnce(Return(DEST_HANDLE));

			EXPECT_CALL(engine_, close_file(SRC_HANDLE)).Times(1);
			EXPECT_CALL(engine_, close_file(DEST_HANDLE)).Times(1);

			return unique_ptr<copier>(new copier(engine_, src_file_,
							     dest_file_,
							     BLOCK_SIZE, 1 * 1024 * 1024));
		}

		static optional<wait_result> make_wr(bool success, unsigned context) {
			return optional<wait_result>(wait_result(success, context));
		}

		void issue_successful_op(copier &c, copy_op &op, unsigned context) {
			InSequence dummy;

			unsigned nr_pending = c.nr_pending();
			EXPECT_CALL(engine_, issue_io(SRC_HANDLE, io_engine::D_READ,
						      op.src_b * BLOCK_SIZE,
						      op.src_e * BLOCK_SIZE, _, context)).
				WillOnce(Return(true));
			c.issue(op);

			ASSERT_TRUE(c.nr_pending() == nr_pending + 1);

			EXPECT_CALL(engine_, wait()).
				WillOnce(Return(make_wr(true, context)));

			EXPECT_CALL(engine_, issue_io(DEST_HANDLE, io_engine::D_WRITE,
						      op.dest_b * BLOCK_SIZE,
						      (op.dest_b + (op.src_e - op.src_b)) * BLOCK_SIZE, _, context)).
				WillOnce(Return(true));

			EXPECT_CALL(engine_, wait()).
				WillOnce(Return(make_wr(true, context)));

			auto mop = c.wait();
			ASSERT_EQ(c.nr_pending(), nr_pending);

			ASSERT_TRUE(mop->success());
		}

		unsigned const SRC_HANDLE = 10;
		unsigned const DEST_HANDLE = 11;

		string src_file_;
		string dest_file_;
		StrictMock<io_engine_mock> engine_;
	};
}

//----------------------------------------------------------------

TEST_F(CopierTests, empty_test)
{
	auto c = make_copier();
}

TEST_F(CopierTests, successful_copy)
{
 	// Copy first block
	copy_op op1(0, 1, 0);

	auto c = make_copier();
	issue_successful_op(*c, op1, 0);
}

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

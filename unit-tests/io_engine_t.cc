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
#include "block-cache/mem_pool.h"
#include "block-cache/io_engine.h"
#include "test_utils.h"


#include <fcntl.h>

using namespace boost;
using namespace std;
using namespace test;
using namespace testing;

//----------------------------------------------------------------

namespace {
	unsigned const MAX_IO = 64;
	unsigned const PAGE_SIZE = 4096;

	class IOEngineTests : public Test {
	public:
		IOEngineTests()
			: pool_(64 * 512, 128 * 512, PAGE_SIZE),
			  src_file_("copy_src", 32),
			  dest_file_("copy_dest", 32),
			  engine_(new aio_engine(MAX_IO)) {
		}

		// in sectors
		static uint64_t meg(unsigned n) {
			return 2 * 1024 * n;
		}

		mempool pool_;
		temp_file src_file_;
		temp_file dest_file_;
		unique_ptr<io_engine> engine_;
	};
}

//----------------------------------------------------------------

TEST_F(IOEngineTests, empty_test)
{
}

TEST_F(IOEngineTests, open_and_close)
{
	auto src_handle = engine_->open_file(src_file_.get_path(), io_engine::READ_ONLY);
	auto dest_handle = engine_->open_file(dest_file_.get_path(), io_engine::READ_WRITE);
	ASSERT_TRUE(src_handle != dest_handle);
	engine_->close_file(src_handle);
	engine_->close_file(dest_handle);
}

TEST_F(IOEngineTests, you_can_read_a_read_only_handle)
{
	unsigned nr_sectors = 8;
	auto src_handle = engine_->open_file(src_file_.get_path(), io_engine::READ_ONLY);
	void *data = pool_.alloc();
	bool r = engine_->issue_io(src_handle,
				   io_engine::READ,
				   0, nr_sectors,
				   data,
				   123);
	ASSERT_TRUE(r);
	auto wr = engine_->wait();
	ASSERT_TRUE(wr.first);
	ASSERT_TRUE(wr.second == 123);

	engine_->close_file(src_handle);
	pool_.free(data);
}


TEST_F(IOEngineTests, you_cannot_write_to_a_read_only_handle)
{
	unsigned nr_sectors = 8;
	auto src_handle = engine_->open_file(src_file_.get_path(), io_engine::READ_ONLY);
	void *data = pool_.alloc();
	bool r = engine_->issue_io(src_handle,
				   io_engine::WRITE,
				   0, nr_sectors,
				   data,
				   0);
	ASSERT_FALSE(r);
	engine_->close_file(src_handle);
	pool_.free(data);
}

TEST_F(IOEngineTests, you_can_write_to_a_read_write_handle)
{
	unsigned nr_sectors = 8;
	auto src_handle = engine_->open_file(src_file_.get_path(), io_engine::READ_ONLY);
	void *data = pool_.alloc();
	bool r = engine_->issue_io(src_handle,
				   io_engine::READ,
				   0, nr_sectors,
				   data,
				   123);
	ASSERT_TRUE(r);
	auto wr = engine_->wait();
	ASSERT_TRUE(wr.first);
	ASSERT_TRUE(wr.second == 123);

	engine_->close_file(src_handle);
	pool_.free(data);
}

TEST_F(IOEngineTests, final_block_read_succeeds)
{
	unsigned nr_sectors = 8;
	auto src_handle = engine_->open_file(src_file_.get_path(), io_engine::READ_ONLY);
	void *data = pool_.alloc();
	bool r = engine_->issue_io(src_handle,
				   io_engine::READ,
				   meg(32) - nr_sectors, meg(32),
				   data,
				   123);
	ASSERT_TRUE(r);
	auto wr = engine_->wait();
	ASSERT_TRUE(wr.first);

	engine_->close_file(src_handle);
	pool_.free(data);

}

TEST_F(IOEngineTests, out_of_bounds_read_fails)
{
	unsigned nr_sectors = 8;
	auto src_handle = engine_->open_file(src_file_.get_path(), io_engine::READ_ONLY);
	void *data = pool_.alloc();
	bool r = engine_->issue_io(src_handle,
				   io_engine::READ,
				   meg(32), meg(32) + nr_sectors,
				   data,
				   123);
	ASSERT_TRUE(r);
	auto wr = engine_->wait();
	ASSERT_FALSE(wr.first);

	engine_->close_file(src_handle);
	pool_.free(data);

}

TEST_F(IOEngineTests, out_of_bounds_write_succeeds)
{
	unsigned nr_sectors = 8;
	auto handle = engine_->open_file(dest_file_.get_path(), io_engine::READ_WRITE);
	void *data = pool_.alloc();
	bool r = engine_->issue_io(handle,
				   io_engine::WRITE,
				   meg(32), meg(32) + nr_sectors,
				   data,
				   123);
	ASSERT_TRUE(r);
	auto wr = engine_->wait();
	ASSERT_TRUE(wr.first);

	engine_->close_file(handle);
	pool_.free(data);

}

//----------------------------------------------------------------

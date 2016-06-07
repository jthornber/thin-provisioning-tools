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
#include "test_utils.h"

#include <fcntl.h>

using namespace boost;
using namespace std;
using namespace test;
using namespace testing;

//----------------------------------------------------------------

namespace {
	class MempoolTests : public Test {
	public:
		bool aligned(void *data, size_t alignment) {
			return (reinterpret_cast<size_t>(data) % alignment) == 0;
		}

	private:

	};
}

//----------------------------------------------------------------

TEST_F(MempoolTests, empty_test)
{
}

TEST_F(MempoolTests, create_destroy_cycle)
{
	for (size_t bs = 64; bs <= 512; bs *= 2) {
		mempool mp(bs, 4 * 1024 * 1024, bs);
	}
}

TEST_F(MempoolTests, alignments_observed)
{
	for (size_t bs = 64; bs <= 512; bs *= 2) {
		mempool mp(bs, 512 * 1024, bs);

		for (unsigned i = 0; i < 100; i++) {
			auto md = mp.alloc();

			if (!md)
				throw runtime_error("couldn't alloc");

			ASSERT_TRUE(aligned(md, bs));
		}
	}
}

TEST_F(MempoolTests, alloc_free_cycle)
{
	mempool mp(512, 512 * 1024, 512);

	for (unsigned i = 0; i < 10000; i++) {
		auto md = mp.alloc();
		mp.free(md);
	}
}

TEST_F(MempoolTests, exhaust_pool)
{
	mempool mp(512, 100 * 512, 512);

	for (unsigned i = 0; i < 100; i++) {
		auto md = mp.alloc();
		ASSERT_NE(md, nullptr);
	}

	auto md = mp.alloc();
	ASSERT_EQ(md, nullptr);
}

// Use valgrind
TEST_F(MempoolTests, data_can_be_written)
{
	mempool mp(512, 100 * 512, 512);

	for (unsigned i = 0; i < 100; i++) {
		auto md = mp.alloc();
		ASSERT_NE(md, nullptr);

		memset(md, 0, 512);
	}

	auto md = mp.alloc();
	ASSERT_EQ(md, nullptr);
}

//----------------------------------------------------------------

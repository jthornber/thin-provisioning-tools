// Copyright (C) 2017 Red Hat, Inc. All rights reserved.
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
#include "block-cache/block_cache.h"
#include "test_utils.h"

#include <stdlib.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

using namespace std;
using namespace test;
using namespace testing;

//----------------------------------------------------------------

TEST(BCacheTests, cleaned_on_demand)
{
	using namespace bcache;

	unsigned const NR_BLOCKS = 16;

	temp_file tmp("bcache_t", 1);
	int fd = open(tmp.get_path().c_str(), O_RDWR | O_DIRECT, 0666);

	uint64_t bs = 8;
	block_cache bc(fd, bs, 64, (bs << SECTOR_SHIFT) * NR_BLOCKS);
	validator::ptr v(new bcache::noop_validator());
	for (unsigned i = 0; i < NR_BLOCKS; i++) {
		block_cache::block &b = bc.get(i, block_cache::GF_DIRTY | block_cache::GF_ZERO, v);
		b.put();
	}

	// Default WRITEBACK_LOW_THRESHOLD_PERCENT == 33
	// For NR_BLOCKS cache entires, only 6 dirty blocks will be submitted,
	// so the 7th get() doesn't work.
	list<block_cache::block *> blocks;

	try {
		for (unsigned i = 0; i < NR_BLOCKS; i++)
			blocks.push_back(&bc.get(NR_BLOCKS + i, 0, v));

	} catch (...) {
		for (auto b : blocks)
			b->put();

		throw;
	}

	for (auto b : blocks)
		b->put();
}


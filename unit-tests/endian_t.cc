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
#include "base/endian_utils.h"

using namespace base;
using namespace std;
using namespace testing;

//----------------------------------------------------------------

TEST(EndianTests, bitmaps)
{
	unsigned NR_BITS = 10247;
	vector<uint64_t> data((NR_BITS + 63) / 64, 0);

	// check all bits are zero
	void *bits = &data[0];
	for (unsigned i = 0; i < NR_BITS; i++)
		ASSERT_FALSE(test_bit_le(bits, i));

	// set all bits to one
	for (unsigned i = 0; i < NR_BITS; i++)
		set_bit_le(bits, i);

	// check they're all 1 now
	for (unsigned i = 0; i < NR_BITS; i++)
		ASSERT_TRUE(test_bit_le(bits, i));

	// clear every third bit
	for (unsigned i = 0; i < NR_BITS; i += 3)
		clear_bit_le(bits, i);

	// check everything is as we expect
	for (unsigned i = 0; i < NR_BITS; i++) {
		if ((i % 3) == 0)
			ASSERT_FALSE(test_bit_le(bits, i));
		else
			ASSERT_TRUE(test_bit_le(bits, i));
	}
}

TEST(EndianTests, bitmaps_alternate_words)
{
	unsigned NR_BITS = 10247;
	vector<uint64_t> data((NR_BITS + 63) / 64, 0);

	// check all bits are zero
	void *bits = &data[0];
	for (unsigned i = 0; i < 128; i++)
		ASSERT_FALSE(test_bit_le(bits, i));

	for (unsigned i = 0; i < 64; i++)
		set_bit_le(bits, i);

	for (unsigned i = 64; i < 128; i++)
		ASSERT_FALSE(test_bit_le(bits, i));
}

//----------------------------------------------------------------

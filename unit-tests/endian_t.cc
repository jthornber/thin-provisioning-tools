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

#include "persistent-data/space_map_disk.h"

#define BOOST_TEST_MODULE EndianTests
#include <boost/test/included/unit_test.hpp>

using namespace base;
using namespace boost;
using namespace persistent_data;
using namespace std;

//----------------------------------------------------------------

BOOST_AUTO_TEST_CASE(bitmaps)
{
	unsigned NR_BITS = 10247;
	vector<uint64_t> data((NR_BITS + 63) / 64, 0);

	// check all bits are zero
	void *bits = &data[0];
	for (unsigned i = 0; i < NR_BITS; i++)
		BOOST_CHECK(!test_bit_le(bits, i));

	// set all bits to one
	for (unsigned i = 0; i < NR_BITS; i++)
		set_bit_le(bits, i);

	// check they're all 1 now
	for (unsigned i = 0; i < NR_BITS; i++)
		BOOST_CHECK(test_bit_le(bits, i));

	// clear every third bit
	for (unsigned i = 0; i < NR_BITS; i += 3)
		clear_bit_le(bits, i);

	// check everything is as we expect
	for (unsigned i = 0; i < NR_BITS; i++) {
		if ((i % 3) == 0)
			BOOST_CHECK(!test_bit_le(bits, i));
		else
			BOOST_CHECK(test_bit_le(bits, i));
	}
}

BOOST_AUTO_TEST_CASE(bitmaps_alternate_words)
{
	unsigned NR_BITS = 10247;
	vector<uint64_t> data((NR_BITS + 63) / 64, 0);

	// check all bits are zero
	void *bits = &data[0];
	for (unsigned i = 0; i < 128; i++)
		BOOST_CHECK(!test_bit_le(bits, i));

	for (unsigned i = 0; i < 64; i++)
		set_bit_le(bits, i);

	for (unsigned i = 64; i < 128; i++)
		BOOST_CHECK(!test_bit_le(bits, i));
}

//----------------------------------------------------------------

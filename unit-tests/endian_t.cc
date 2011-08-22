#include "space_map_disk.h"
#include "core_map.h"

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

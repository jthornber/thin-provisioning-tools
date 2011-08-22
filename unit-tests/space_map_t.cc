#include "space_map.h"
#include "core_map.h"

#define BOOST_TEST_MODULE SpaceMapTests
#include <boost/test/included/unit_test.hpp>

using namespace std;
using namespace boost;

unsigned const NR_BLOCKS = 1024;

//----------------------------------------------------------------

BOOST_AUTO_TEST_CASE(test_get_nr_blocks)
{
	core_map sm(NR_BLOCKS);
	BOOST_CHECK_EQUAL(sm.get_nr_blocks(), NR_BLOCKS);
}

BOOST_AUTO_TEST_CASE(test_get_nr_free)
{
	core_map sm(NR_BLOCKS);
	BOOST_CHECK_EQUAL(sm.get_nr_free(), NR_BLOCKS);

	for (unsigned i = 0; i < NR_BLOCKS; i++) {
		sm.new_block();
		BOOST_CHECK_EQUAL(sm.get_nr_free(), NR_BLOCKS - i - 1);
	}

	for (unsigned i = 0; i < NR_BLOCKS; i++) {
		sm.dec(i);
		BOOST_CHECK_EQUAL(sm.get_nr_free(), i + 1);
	}
}

BOOST_AUTO_TEST_CASE(test_throws_no_space)
{
	core_map sm(NR_BLOCKS);
	for (unsigned i = 0; i < NR_BLOCKS; i++)
		sm.new_block();

	BOOST_CHECK_THROW(sm.new_block(), std::runtime_error);
}

BOOST_AUTO_TEST_CASE(test_inc_and_dec)
{
	core_map sm(NR_BLOCKS);
	block_address b = 63;

	for (unsigned i = 0; i < 50; i++) {
		BOOST_CHECK_EQUAL(sm.get_count(b), i);
		sm.inc(b);
	}

	for (unsigned i = 50; i > 0; i--) {
		BOOST_CHECK_EQUAL(sm.get_count(b), i);
		sm.dec(b);
	}
}

BOOST_AUTO_TEST_CASE(test_not_allocated_twice)
{
	core_map sm(NR_BLOCKS);
	block_address b = sm.new_block();

	try {
		for (;;)
			BOOST_CHECK(sm.new_block() != b);
	} catch (...) {
	}
}

BOOST_AUTO_TEST_CASE(test_set_count)
{
	core_map sm(NR_BLOCKS);
	sm.set_count(43, 5);
	BOOST_CHECK_EQUAL(sm.get_count(43), 5);
}

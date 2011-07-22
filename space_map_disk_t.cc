#include "space_map_disk.h"
#include "core_map.h"

#define BOOST_TEST_MODULE SpaceMapDiskTests
#include <boost/test/included/unit_test.hpp>

using namespace std;
using namespace boost;
using namespace persistent_data;

//----------------------------------------------------------------

namespace {
	block_address const NR_BLOCKS = 10237;
	block_address const SUPERBLOCK = 0;
	unsigned const BLOCK_SIZE = 4096;

	transaction_manager<BLOCK_SIZE>::ptr
	create_tm() {
		block_manager<BLOCK_SIZE>::ptr bm(
			new block_manager<BLOCK_SIZE>("./test.data", NR_BLOCKS));
		space_map::ptr sm(new core_map(1024));
		transaction_manager<BLOCK_SIZE>::ptr tm(
			new transaction_manager<BLOCK_SIZE>(bm, sm));
		return tm;
	}

	persistent_space_map::ptr
	create_sm_disk() {
		auto tm = create_tm();
		return persistent_data::create_disk_sm<BLOCK_SIZE>(tm, NR_BLOCKS);
	}
}

//----------------------------------------------------------------

BOOST_AUTO_TEST_CASE(reopen_an_sm)
{
	auto sm = create_sm_disk();
}

BOOST_AUTO_TEST_CASE(test_get_nr_blocks)
{
	auto sm = create_sm_disk();
	BOOST_CHECK_EQUAL(sm->get_nr_blocks(), NR_BLOCKS);
}

BOOST_AUTO_TEST_CASE(test_get_nr_free)
{
	auto sm = create_sm_disk();
	BOOST_CHECK_EQUAL(sm->get_nr_free(), NR_BLOCKS);

	for (unsigned i = 0; i < NR_BLOCKS; i++) {
		sm->new_block();
		BOOST_CHECK_EQUAL(sm->get_nr_free(), NR_BLOCKS - i - 1);
	}

	for (unsigned i = 0; i < NR_BLOCKS; i++) {
		sm->dec(i);
		BOOST_CHECK_EQUAL(sm->get_nr_free(), i + 1);
	}
}

BOOST_AUTO_TEST_CASE(test_throws_no_space)
{
	auto sm = create_sm_disk();
	for (unsigned i = 0; i < NR_BLOCKS; i++)
		sm->new_block();

	BOOST_CHECK_THROW(sm->new_block(), std::runtime_error);
}

BOOST_AUTO_TEST_CASE(test_inc_and_dec)
{
	auto sm = create_sm_disk();
	block_address b = 63;

	for (unsigned i = 0; i < 50; i++) {
		BOOST_CHECK_EQUAL(sm->get_count(b), i);
		sm->inc(b);
	}

	for (unsigned i = 50; i > 0; i--) {
		BOOST_CHECK_EQUAL(sm->get_count(b), i);
		sm->dec(b);
	}
}

BOOST_AUTO_TEST_CASE(test_not_allocated_twice)
{
	auto sm = create_sm_disk();
	block_address b = sm->new_block();

	try {
		for (;;)
			BOOST_CHECK(sm->new_block() != b);
	} catch (...) {
	}
}

BOOST_AUTO_TEST_CASE(test_set_count)
{
	auto sm = create_sm_disk();
	sm->set_count(43, 5);
	BOOST_CHECK_EQUAL(sm->get_count(43), 5);
}

BOOST_AUTO_TEST_CASE(test_set_effects_nr_allocated)
{
	auto sm = create_sm_disk();
	for (unsigned i = 0; i < NR_BLOCKS; i++) {
		sm->set_count(i, 1);
		BOOST_CHECK_EQUAL(sm->get_nr_free(), NR_BLOCKS - i - 1);
	}

	for (unsigned i = 0; i < NR_BLOCKS; i++) {
		sm->set_count(i, 0);
		BOOST_CHECK_EQUAL(sm->get_nr_free(), i + 1);
	}
}

BOOST_AUTO_TEST_CASE(test_reopen)
{
	unsigned char buffer[128];

	{
		auto sm = create_sm_disk();
		for (unsigned i = 0, step = 1; i < NR_BLOCKS; i += step, step++) {
			sm->inc(i);
		}

		BOOST_CHECK(sm->root_size() <= sizeof(buffer));

		sm->copy_root(buffer, sizeof(buffer));
	}

	{
		auto tm = create_tm();
		auto sm = persistent_data::open_disk_sm<BLOCK_SIZE>(tm, buffer);

		for (unsigned i = 0, step = 1; i < NR_BLOCKS; i += step, step++)
			BOOST_CHECK_EQUAL(sm->get_count(i), 1);
	}
}

//----------------------------------------------------------------

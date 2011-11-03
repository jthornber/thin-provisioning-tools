#include "space_map_disk.h"
#include "space_map_core.h"

#define BOOST_TEST_MODULE SpaceMapDiskTests
#include <boost/test/included/unit_test.hpp>

using namespace std;
using namespace boost;
using namespace persistent_data;

//----------------------------------------------------------------

namespace {
	block_address const NR_BLOCKS = 1000; // FIXME: bump up
	block_address const SUPERBLOCK = 0;
	block_address const MAX_LOCKS = 8;

	transaction_manager::ptr
	create_tm() {
		block_manager<>::ptr bm(
			new block_manager<>("./test.data", NR_BLOCKS, MAX_LOCKS, true));
		space_map::ptr sm(new core_map(1024));
		transaction_manager::ptr tm(
			new transaction_manager(bm, sm));
		return tm;
	}

	persistent_space_map::ptr
	create_sm_disk() {
		transaction_manager::ptr tm = create_tm();
		return persistent_data::create_disk_sm(tm, NR_BLOCKS);
	}

	persistent_space_map::ptr
	create_sm_metadata() {
		transaction_manager::ptr tm = create_tm();
		return persistent_data::create_metadata_sm(tm, NR_BLOCKS);
	}
}

//----------------------------------------------------------------

void _test_get_nr_blocks(space_map::ptr sm)
{
	BOOST_CHECK_EQUAL(sm->get_nr_blocks(), NR_BLOCKS);
}

void _test_get_nr_free(space_map::ptr sm)
{
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

void _test_throws_no_space(space_map::ptr sm)
{
	for (unsigned i = 0; i < NR_BLOCKS; i++)
		sm->new_block();

	BOOST_CHECK_THROW(sm->new_block(), std::runtime_error);
}

void _test_inc_and_dec(space_map::ptr sm)
{
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

void _test_not_allocated_twice(space_map::ptr sm)
{
	block_address b = sm->new_block();

	try {
		for (;;)
			BOOST_CHECK(sm->new_block() != b);
	} catch (...) {
	}
}

void _test_set_count(space_map::ptr sm)
{
	sm->set_count(43, 5);
	BOOST_CHECK_EQUAL(sm->get_count(43), 5);
}

void _test_set_affects_nr_allocated(space_map::ptr sm)
{
	for (unsigned i = 0; i < NR_BLOCKS; i++) {
		sm->set_count(i, 1);
		BOOST_CHECK_EQUAL(sm->get_nr_free(), NR_BLOCKS - i - 1);
	}

	for (unsigned i = 0; i < NR_BLOCKS; i++) {
		sm->set_count(i, 0);
		BOOST_CHECK_EQUAL(sm->get_nr_free(), i + 1);
	}
}

//----------------------------------------------------------------

#if 0
BOOST_AUTO_TEST_CASE(reopen_an_sm)
{
	space_map::ptr sm = create_sm_disk();
}
#endif

BOOST_AUTO_TEST_CASE(test_disk_get_nr_blocks)
{
	space_map::ptr sm = create_sm_disk();
	_test_get_nr_blocks(sm);
}

BOOST_AUTO_TEST_CASE(test_disk_get_nr_free)
{
	space_map::ptr sm = create_sm_disk();
	_test_get_nr_free(sm);
}

BOOST_AUTO_TEST_CASE(test_disk_throws_no_space)
{
	space_map::ptr sm = create_sm_disk();
	_test_throws_no_space(sm);
}

BOOST_AUTO_TEST_CASE(test_disk_inc_and_dec)
{
	space_map::ptr sm = create_sm_disk();
	_test_inc_and_dec(sm);
}

BOOST_AUTO_TEST_CASE(test_disk_not_allocated_twice)
{
	space_map::ptr sm = create_sm_disk();
	_test_not_allocated_twice(sm);
}

BOOST_AUTO_TEST_CASE(test_disk_set_count)
{
	space_map::ptr sm = create_sm_disk();
	_test_set_count(sm);
}

BOOST_AUTO_TEST_CASE(test_disk_set_affects_nr_allocated)
{
	space_map::ptr sm = create_sm_disk();
	_test_set_affects_nr_allocated(sm);
}

BOOST_AUTO_TEST_CASE(test_disk_reopen)
{
	unsigned char buffer[128];

	{
		persistent_space_map::ptr sm = create_sm_disk();
		for (unsigned i = 0, step = 1; i < NR_BLOCKS; i += step, step++) {
			sm->inc(i);
		}
		sm->commit();

		BOOST_CHECK(sm->root_size() <= sizeof(buffer));
		sm->copy_root(buffer, sizeof(buffer));
	}

	{
		transaction_manager::ptr tm = create_tm();
		persistent_space_map::ptr sm = persistent_data::open_disk_sm(tm, buffer);

		for (unsigned i = 0, step = 1; i < NR_BLOCKS; i += step, step++)
			BOOST_CHECK_EQUAL(sm->get_count(i), 1);
	}
}

//----------------------------------------------------------------

BOOST_AUTO_TEST_CASE(test_metadata_get_nr_blocks)
{
	space_map::ptr sm = create_sm_metadata();
	_test_get_nr_blocks(sm);
}

BOOST_AUTO_TEST_CASE(test_metadata_get_nr_free)
{
	space_map::ptr sm = create_sm_metadata();
	_test_get_nr_free(sm);
}

BOOST_AUTO_TEST_CASE(test_metadata_throws_no_space)
{
	space_map::ptr sm = create_sm_metadata();
	_test_throws_no_space(sm);
}

BOOST_AUTO_TEST_CASE(test_metadata_inc_and_dec)
{
	space_map::ptr sm = create_sm_metadata();
	_test_inc_and_dec(sm);
}

BOOST_AUTO_TEST_CASE(test_metadata_not_allocated_twice)
{
	space_map::ptr sm = create_sm_metadata();
	_test_not_allocated_twice(sm);
}

BOOST_AUTO_TEST_CASE(test_metadata_set_count)
{
	space_map::ptr sm = create_sm_metadata();
	_test_set_count(sm);
}

BOOST_AUTO_TEST_CASE(test_metadata_set_affects_nr_allocated)
{
	space_map::ptr sm = create_sm_metadata();
	_test_set_affects_nr_allocated(sm);
}

BOOST_AUTO_TEST_CASE(test_metadata_reopen)
{
	unsigned char buffer[128];

	{
		persistent_space_map::ptr sm = create_sm_metadata();
		for (unsigned i = 0, step = 1; i < NR_BLOCKS; i += step, step++) {
			sm->inc(i);
		}
		sm->commit();

		BOOST_CHECK(sm->root_size() <= sizeof(buffer));
		sm->copy_root(buffer, sizeof(buffer));
	}

	{
		transaction_manager::ptr tm = create_tm();
		persistent_space_map::ptr sm = persistent_data::open_metadata_sm(tm, buffer);

		for (unsigned i = 0, step = 1; i < NR_BLOCKS; i += step, step++)
			BOOST_CHECK_EQUAL(sm->get_count(i), 1);
	}
}

//----------------------------------------------------------------

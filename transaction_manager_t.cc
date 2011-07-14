#include "transaction_manager.h"
#include "core_map.h"

#define BOOST_TEST_MODULE TransactionManagerTests
#include <boost/test/included/unit_test.hpp>

using namespace std;
using namespace boost;
using namespace persistent_data;

//----------------------------------------------------------------

namespace {
	block_address const NR_BLOCKS = 1024;

	transaction_manager<4096>::ptr
	create_tm() {
		block_manager<4096>::ptr bm(new block_manager<4096>("./test.data", NR_BLOCKS));
		space_map::ptr sm(new core_map(NR_BLOCKS));
		transaction_manager<4096>::ptr tm(new transaction_manager<4096>(bm, sm));
		return tm;
	}
}

//----------------------------------------------------------------

BOOST_AUTO_TEST_CASE(commit_succeeds)
{
	auto tm = create_tm();
	tm->begin(0);
}

BOOST_AUTO_TEST_CASE(shadowing)
{
	auto tm = create_tm();
	auto superblock = tm->begin(0);

	auto sm = tm->get_sm();
	sm->inc(1);
	auto p = tm->shadow(1);
	auto b = p.first.get_location();
	BOOST_CHECK(b != 1);
	BOOST_CHECK(!p.second);
	BOOST_CHECK(sm->get_count(1) == 0);

	p = tm->shadow(b);
	BOOST_CHECK(p.first.get_location() == b);
	BOOST_CHECK(!p.second);

	sm->inc(b);
	p = tm->shadow(b);
	BOOST_CHECK(p.first.get_location() != b);
	BOOST_CHECK(p.second);
}

BOOST_AUTO_TEST_CASE(multiple_shadowing)
{
	auto tm = create_tm();
	auto superblock = tm->begin(0);

	auto sm = tm->get_sm();
	sm->set_count(1, 3);

	auto p = tm->shadow(1);
	auto b = p.first.get_location();
	BOOST_CHECK(b != 1);
	BOOST_CHECK(p.second);

	p = tm->shadow(1);
	auto b2 = p.first.get_location();
	BOOST_CHECK(b2 != 1);
	BOOST_CHECK(b2 != b);
	BOOST_CHECK(p.second);

	p = tm->shadow(1);
	auto b3 = p.first.get_location();
	BOOST_CHECK(b3 != b2);
	BOOST_CHECK(b3 != b);
	BOOST_CHECK(b3 != 1);
	BOOST_CHECK(!p.second);
}

BOOST_AUTO_TEST_CASE(shadow_free_block_fails)
{
	auto tm = create_tm();
	auto superblock = tm->begin(0);
	BOOST_CHECK_THROW(tm->shadow(1), runtime_error);
}

// First shadow in a transaction returns a different block
// Second shadow in a transaction rtu

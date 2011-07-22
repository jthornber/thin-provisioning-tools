#include "transaction_manager.h"
#include "core_map.h"
#include "btree.h"

#define BOOST_TEST_MODULE BTreeTests
#include <boost/test/included/unit_test.hpp>

using namespace std;
using namespace boost;
using namespace persistent_data;

//----------------------------------------------------------------

namespace {
	block_address const NR_BLOCKS = 102400;

	transaction_manager<4096>::ptr
	create_tm() {
		block_manager<4096>::ptr bm(new block_manager<4096>("./test.data", NR_BLOCKS));
		space_map::ptr sm(new core_map(NR_BLOCKS));
		transaction_manager<4096>::ptr tm(new transaction_manager<4096>(bm, sm));
		return tm;
	}

	btree<1, uint64_traits, 4096>::ptr
	create_btree() {
		typename uint64_traits::ref_counter rc;

		return btree<1, uint64_traits, 4096>::ptr(
			new btree<1, uint64_traits, 4096>(
				create_tm(), rc));
	}
}

//----------------------------------------------------------------

BOOST_AUTO_TEST_CASE(empty_btree_contains_nothing)
{
	auto tree = create_btree();

	for (uint64_t i = 0; i < 1000; i++) {
		uint64_t key[1] = {i};
		BOOST_CHECK(!tree->lookup(key));
	}
}

BOOST_AUTO_TEST_CASE(insert_works)
{
	unsigned const COUNT = 1000000;

	auto tree = create_btree();
	for (uint64_t i = 0; i < COUNT; i++) {
		uint64_t key[1] = {i * 7};
		uint64_t value = i;
		tree->insert(key, value);

		auto l = tree->lookup(key);
		BOOST_CHECK(l);
		BOOST_CHECK_EQUAL(*l, i);
	}
}

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

	transaction_manager::ptr
	create_tm() {
		block_manager<>::ptr bm(new block_manager<>("./test.data", NR_BLOCKS));
		space_map::ptr sm(new core_map(NR_BLOCKS));
		transaction_manager::ptr tm(new transaction_manager(bm, sm));
		tm->get_sm()->inc(0);
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
	block_address b;

	{
		auto p = tm->shadow(1);
		b = p.first.get_location();
		BOOST_CHECK(b != 1);
		BOOST_CHECK(!p.second);
		BOOST_CHECK(sm->get_count(1) == 0);
	}

	{
		auto p = tm->shadow(b);
		BOOST_CHECK(p.first.get_location() == b);
		BOOST_CHECK(!p.second);
	}

	sm->inc(b);

	{
		auto p = tm->shadow(b);
		BOOST_CHECK(p.first.get_location() != b);
		BOOST_CHECK(p.second);
	}
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


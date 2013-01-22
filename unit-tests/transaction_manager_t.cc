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

#include "persistent-data/transaction_manager.h"
#include "persistent-data/space-maps/core.h"

#define BOOST_TEST_MODULE TransactionManagerTests
#include <boost/test/included/unit_test.hpp>

using namespace std;
using namespace boost;
using namespace persistent_data;

//----------------------------------------------------------------

namespace {
	unsigned const MAX_HELD_LOCKS = 16;
	block_address const NR_BLOCKS = 1024;

	transaction_manager::ptr
	create_tm() {
		block_manager<>::ptr bm(
			new block_manager<>("./test.data", NR_BLOCKS, MAX_HELD_LOCKS, block_io<>::READ_WRITE));
		space_map::ptr sm(new core_map(NR_BLOCKS));
		transaction_manager::ptr tm(new transaction_manager(bm, sm));
		tm->get_sm()->inc(0);
		return tm;
	}

	typedef block_manager<>::validator::ptr validator_ptr;

	validator_ptr noop_validator() {
		return block_manager<>::validator::ptr(
			new block_manager<>::noop_validator);
	}

	typedef block_manager<>::write_ref write_ref;
}

//----------------------------------------------------------------

BOOST_AUTO_TEST_CASE(commit_succeeds)
{
	transaction_manager::ptr tm = create_tm();
	tm->begin(0, noop_validator());
}

BOOST_AUTO_TEST_CASE(shadowing)
{
	transaction_manager::ptr tm = create_tm();
	block_manager<>::write_ref superblock = tm->begin(0, noop_validator());

	space_map::ptr sm = tm->get_sm();
	sm->inc(1);
	block_address b;

	{
		pair<write_ref, bool> p = tm->shadow(1, noop_validator());
		b = p.first.get_location();
		BOOST_CHECK(b != 1);
		BOOST_CHECK(!p.second);
		BOOST_CHECK(sm->get_count(1) == 0);
	}

	{
		pair<write_ref, bool> p = tm->shadow(b, noop_validator());
		BOOST_CHECK(p.first.get_location() == b);
		BOOST_CHECK(!p.second);
	}

	sm->inc(b);

	{
		pair<write_ref, bool> p = tm->shadow(b, noop_validator());
		BOOST_CHECK(p.first.get_location() != b);
		BOOST_CHECK(p.second);
	}
}

BOOST_AUTO_TEST_CASE(multiple_shadowing)
{
	transaction_manager::ptr tm = create_tm();
	space_map::ptr sm = tm->get_sm();
	sm->set_count(1, 3);
	block_address b, b2;

	{
		write_ref superblock = tm->begin(0, noop_validator());
		pair<write_ref, bool> p = tm->shadow(1, noop_validator());
		b = p.first.get_location();
		BOOST_CHECK(b != 1);
		BOOST_CHECK(p.second);
		sm->commit();
	}

	{
		write_ref superblock = tm->begin(0, noop_validator());
		pair<write_ref, bool> p = tm->shadow(1, noop_validator());
		b2 = p.first.get_location();
		BOOST_CHECK(b2 != 1);
		BOOST_CHECK(b2 != b);
		BOOST_CHECK(p.second);
		sm->commit();
	}

	{
		write_ref superblock = tm->begin(0, noop_validator());
		pair<write_ref, bool> p = tm->shadow(1, noop_validator());
		block_address b3 = p.first.get_location();
		BOOST_CHECK(b3 != b2);
		BOOST_CHECK(b3 != b);
		BOOST_CHECK(b3 != 1);
		BOOST_CHECK(!p.second);
		sm->commit();
	}
}

BOOST_AUTO_TEST_CASE(shadow_free_block_fails)
{
	transaction_manager::ptr tm = create_tm();
	write_ref superblock = tm->begin(0, noop_validator());
	BOOST_CHECK_THROW(tm->shadow(1, noop_validator()), runtime_error);
}

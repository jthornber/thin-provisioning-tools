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
#include "persistent-data/transaction_manager.h"
#include "persistent-data/space-maps/core.h"

using namespace std;
using namespace persistent_data;
using namespace testing;

//----------------------------------------------------------------

namespace {
	unsigned const MAX_HELD_LOCKS = 16;
	block_address const NR_BLOCKS = 1024;

	transaction_manager::ptr
	create_tm() {
		block_manager<>::ptr bm(
			new block_manager<>("./test.data", NR_BLOCKS, MAX_HELD_LOCKS, block_manager<>::READ_WRITE));
		space_map::ptr sm(new core_map(NR_BLOCKS));
		transaction_manager::ptr tm(new transaction_manager(bm, sm));
		tm->get_sm()->inc(0);
		return tm;
	}

	typedef bcache::validator::ptr validator_ptr;

	validator_ptr mk_noop_validator() {
		return bcache::validator::ptr(
			new bcache::noop_validator);
	}

	typedef block_manager<>::write_ref write_ref;
}

//----------------------------------------------------------------

TEST(TransactionManagerTests, commit_succeeds)
{
	transaction_manager::ptr tm = create_tm();
	tm->begin(0, mk_noop_validator());
}

TEST(TransactionManagerTests, shadowing)
{
	transaction_manager::ptr tm = create_tm();
	block_manager<>::write_ref superblock = tm->begin(0, mk_noop_validator());

	space_map::ptr sm = tm->get_sm();
	sm->inc(1);
	block_address b;

	{
		pair<write_ref, bool> p = tm->shadow(1, mk_noop_validator());
		b = p.first.get_location();
		ASSERT_THAT(b, Ne(1u));
		ASSERT_FALSE(p.second);
		ASSERT_THAT(sm->get_count(1), Eq(0u));
	}

	{
		pair<write_ref, bool> p = tm->shadow(b, mk_noop_validator());
		ASSERT_THAT(p.first.get_location(), Eq(b));
		ASSERT_FALSE(p.second);
	}

	sm->inc(b);

	{
		pair<write_ref, bool> p = tm->shadow(b, mk_noop_validator());
		ASSERT_THAT(p.first.get_location(), Ne(b));
		ASSERT_TRUE(p.second);
	}
}

TEST(TransactionManagerTests, multiple_shadowing)
{
	transaction_manager::ptr tm = create_tm();
	space_map::ptr sm = tm->get_sm();
	sm->set_count(1, 3);
	block_address b, b2;

	{
		write_ref superblock = tm->begin(0, mk_noop_validator());
		pair<write_ref, bool> p = tm->shadow(1, mk_noop_validator());
		b = p.first.get_location();
		ASSERT_THAT(b, Ne(1u));
		ASSERT_TRUE(p.second);
		sm->commit();
	}

	{
		write_ref superblock = tm->begin(0, mk_noop_validator());
		pair<write_ref, bool> p = tm->shadow(1, mk_noop_validator());
		b2 = p.first.get_location();
		ASSERT_THAT(b2, Ne(1u));
		ASSERT_THAT(b2, Ne(b));
		ASSERT_TRUE(p.second);
		sm->commit();
	}

	{
		write_ref superblock = tm->begin(0, mk_noop_validator());
		pair<write_ref, bool> p = tm->shadow(1, mk_noop_validator());
		block_address b3 = p.first.get_location();
		ASSERT_THAT(b3, Ne(b2));
		ASSERT_THAT(b3, Ne(b));
		ASSERT_THAT(b3, Ne(1u));
		ASSERT_FALSE(p.second);
		sm->commit();
	}
}

TEST(TransactionManagerTests, shadow_free_block_fails)
{
	transaction_manager::ptr tm = create_tm();
	write_ref superblock = tm->begin(0, mk_noop_validator());
	ASSERT_THROW(tm->shadow(1, mk_noop_validator()), runtime_error);
}

//----------------------------------------------------------------

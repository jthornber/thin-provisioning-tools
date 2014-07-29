// Copyright (C) 2013 Red Hat, Inc. All rights reserved.
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
#include "persistent-data/data-structures/array_block.h"
#include "test_utils.h"

#include <utility>
#include <vector>

using namespace persistent_data;
using namespace std;
using namespace test;
using namespace testing;

//----------------------------------------------------------------

namespace {
	uint64_t MAX_VALUE = 1000ull;
	block_address const NR_BLOCKS = 1024;
	typedef typename bcache::noop_validator noop_validator;
	typedef typename block_manager<>::read_ref read_ref;
	typedef typename block_manager<>::write_ref write_ref;

	// FIXME: lift to utils?
	class simple_ref_counter {
	public:
		simple_ref_counter(uint64_t nr_counts)
			: counts_(nr_counts, 0u) {
		}

		void inc(uint64_t n) {
			counts_.at(n)++;
		}

		void dec(uint64_t n) {
			counts_.at(n)--;
		}

		unsigned get(uint64_t n) const {
			return counts_.at(n);
		}

	private:
		vector<unsigned> counts_;
	};

	struct uint64_traits {
		typedef base::le64 disk_type;
		typedef uint64_t value_type;
		typedef simple_ref_counter ref_counter;

		static void unpack(disk_type const &disk, value_type &value) {
			value = base::to_cpu<uint64_t>(disk);
		}

		static void pack(value_type const &value, disk_type &disk) {
			disk = base::to_disk<base::le64>(value);
		}
	};

	typedef array_block<uint64_traits, write_ref> ablock64;
	typedef array_block<uint64_traits, read_ref> ablock64_r;

	bcache::validator::ptr
	validator() {
		return bcache::validator::ptr(new bcache::noop_validator);
	}

	transaction_manager::ptr
	create_tm() {
		block_manager<>::ptr bm = create_bm<4096>(NR_BLOCKS);
		space_map::ptr sm(new core_map(NR_BLOCKS));
		transaction_manager::ptr tm(new transaction_manager(bm, sm));
		return tm;
	}

	pair<ablock64, block_address>
	new_array_block(transaction_manager::ptr tm) {
		uint64_traits::ref_counter rc(MAX_VALUE);
		write_ref wr = tm->new_block(validator());
		ablock64 b(wr, rc);
		b.setup_empty();
		return make_pair(b, wr.get_location());
	}

	ablock64
	open_array_block(transaction_manager::ptr tm, block_address loc) {
		uint64_traits::ref_counter rc(MAX_VALUE);
		pair<write_ref, bool> p = tm->shadow(loc, validator());
		// FIXME: no idea why this isn't working.
//		ASSERT_FALSE(p.second);
		return ablock64(p.first, rc);
	}

	ablock64_r
	read_array_block(transaction_manager::ptr tm, block_address loc) {
		uint64_traits::ref_counter rc(MAX_VALUE);
		read_ref rr = tm->read_lock(loc, validator());
		return ablock64_r(rr, rc);
	}
}

//----------------------------------------------------------------

TEST(ArrayBlockTests, can_create_an_empty_array)
{
	block_address loc;
	transaction_manager::ptr tm = create_tm();

	{
		pair<ablock64, block_address> p = new_array_block(tm);
		ablock64 &b = p.first;
		loc = p.second;

		ASSERT_THAT(b.nr_entries(), Eq(0u));
		ASSERT_THAT(b.value_size(), Eq(sizeof(uint64_t)));
		ASSERT_THAT(b.max_entries(), Eq(static_cast<unsigned>((4096 - 24) / 8)));

		ASSERT_THROW(b.get(0), runtime_error);
		ASSERT_THROW(b.set(0, 12345LL), runtime_error);
	}

	{
		ablock64 b = open_array_block(tm, loc);

		ASSERT_THAT(b.nr_entries(), Eq(0u));
		ASSERT_THAT(b.value_size(), Eq(sizeof(uint64_t)));
		ASSERT_THAT(b.max_entries(), Eq(static_cast<unsigned>((4096 - 24) / 8)));

		ASSERT_THROW(b.get(0), runtime_error);
		ASSERT_THROW(b.set(0, 12345LL), runtime_error);
	}
}

TEST(ArrayBlockTests, read_only_array_blocks_are_possible)
{
	unsigned const COUNT = 10;

	block_address loc;
	transaction_manager::ptr tm = create_tm();

	{
		pair<ablock64, block_address> p = new_array_block(tm);
		ablock64 &b = p.first;
		loc = p.second;

		ASSERT_THAT(b.nr_entries(), Eq(0u));
		ASSERT_THAT(b.value_size(), Eq(sizeof(uint64_t)));
		ASSERT_THAT(b.max_entries(), Eq(static_cast<unsigned>((4096 - 24) / 8)));

		ASSERT_THROW(b.get(0), runtime_error);
		ASSERT_THROW(b.set(0, 12345LL), runtime_error);

		b.grow(COUNT, 0);
		for (unsigned i = 0; i < COUNT; i++)
			b.set(i, i);
	}

	{
		ablock64_r b = read_array_block(tm, loc);

		ASSERT_THAT(b.nr_entries(), Eq(COUNT));
		ASSERT_THAT(b.value_size(), Eq(sizeof(uint64_t)));
		ASSERT_THAT(b.max_entries(), Eq(static_cast<unsigned>((4096 - 24) / 8)));

		for (unsigned i = 0; i < COUNT; i++)
			ASSERT_THAT(b.get(i), Eq(i));
	}
}

TEST(ArrayBlockTests, updating_reopened_array_block)
{
	unsigned const COUNT = 10;

	block_address loc;
	transaction_manager::ptr tm = create_tm();

	{
		pair<ablock64, block_address> p = new_array_block(tm);
		ablock64 &b = p.first;
		loc = p.second;

		ASSERT_THAT(b.nr_entries(), Eq(0u));
		ASSERT_THAT(b.value_size(), Eq(sizeof(uint64_t)));
		ASSERT_THAT(b.max_entries(), Eq(static_cast<unsigned>((4096 - 24) / 8)));

		ASSERT_THROW(b.get(0), runtime_error);
		ASSERT_THROW(b.set(0, 12345LL), runtime_error);

		b.grow(COUNT, 0);
		for (unsigned i = 0; i < COUNT; i++)
			b.set(i, i);
	}

	{
		ablock64 b = open_array_block(tm, loc);

		ASSERT_THAT(b.nr_entries(), Eq(COUNT));
		ASSERT_THAT(b.value_size(), Eq(sizeof(uint64_t)));
		ASSERT_THAT(b.max_entries(), static_cast<unsigned>((4096 - 24) / 8));

		for (unsigned i = 0; i < COUNT; i++)
			ASSERT_THAT(b.get(i), Eq(i));

		for (unsigned i = 0; i < COUNT; i++)
			b.set(i, i + 37);
	}

	{
		ablock64_r b = read_array_block(tm, loc);

		ASSERT_THAT(b.nr_entries(), Eq(COUNT));
		ASSERT_THAT(b.value_size(), Eq(sizeof(uint64_t)));
		ASSERT_THAT(b.max_entries(), static_cast<unsigned>((4096 - 24) / 8));

		for (unsigned i = 0; i < COUNT; i++)
			ASSERT_THAT(b.get(i), Eq(i + 37u));
	}
}

TEST(ArrayBlockTests, growing)
{
	uint64_t default_value = 123, new_value = 234;
	transaction_manager::ptr tm = create_tm();
	pair<ablock64, block_address> p = new_array_block(tm);
	ablock64 &b = p.first;

	for (unsigned i = 1; i < b.max_entries(); i++) {
		ASSERT_THROW(b.get(i - 1), runtime_error);

		b.grow(i, default_value);
		ASSERT_THAT(b.get(i - 1), Eq(default_value));

		b.set(i - 1, new_value);
		ASSERT_THAT(b.get(i - 1), Eq(new_value));

		ASSERT_THROW(b.grow(i - 1, default_value), runtime_error);
	}
}

TEST(ArrayBlockTests, grow_does_not_touch_existing_values)
{
	transaction_manager::ptr tm = create_tm();
	pair<ablock64, block_address> p = new_array_block(tm);
	ablock64 &b = p.first;

	b.grow(10, 123);
	ASSERT_THAT(b.get(9), Eq(123u));
	b.grow(20, 234);
	ASSERT_THAT(b.get(9), Eq(123u));
}

TEST(ArrayBlockTests, shrinking)
{
	uint64_t default_value = 123;
	transaction_manager::ptr tm = create_tm();
	pair<ablock64, block_address> p = new_array_block(tm);
	ablock64 &b = p.first;

	b.grow(b.max_entries() - 1, default_value);
	for (unsigned i = b.max_entries() - 2; i; i--) {
		ASSERT_THAT(b.get(i - 1), Eq(default_value));
		b.shrink(i);
		ASSERT_THROW(b.get(i), runtime_error);
		ASSERT_THROW(b.shrink(i), runtime_error);
	}
}

TEST(ArrayBlockTests, ref_counting)
{
	transaction_manager::ptr tm = create_tm();
	pair<ablock64, block_address> p = new_array_block(tm);
	ablock64 &b = p.first;
	simple_ref_counter const &rc = b.get_ref_counter();

	ASSERT_THAT(rc.get(123), Eq(0u));
	b.grow(b.max_entries() - 1, 123);
	ASSERT_THAT(rc.get(123), Eq(b.max_entries() - 1u));

	b.shrink(100);
	ASSERT_THAT(rc.get(123), Eq(100u));

	b.set(1, 0);
	b.set(2, 2);
	ASSERT_THAT(rc.get(123), Eq(98u));
	ASSERT_THAT(rc.get(0), Eq(1u));

	b.set(2, 2);
	ASSERT_THAT(rc.get(2), Eq(1u));
	b.set(10, 2);
	ASSERT_THAT(rc.get(2), Eq(2u));
	ASSERT_THAT(rc.get(123), Eq(97u));
}

//----------------------------------------------------------------

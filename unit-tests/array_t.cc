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
#include "persistent-data/data-structures/array.h"
#include "persistent-data/data-structures/simple_traits.h"

#include <vector>

using namespace std;
using namespace persistent_data;
using namespace testing;

//----------------------------------------------------------------

namespace {
	block_address const NR_BLOCKS = 102400;
	typedef persistent_data::array<uint64_traits> array64;

	class ArrayTests : public Test {
	public:
		ArrayTests()
			: bm_(new block_manager<>("./test.data", NR_BLOCKS, 4, block_manager<>::READ_WRITE)),
			  sm_(new core_map(NR_BLOCKS)),
			  tm_(bm_, sm_) {
		}

		void
		create_array(unsigned nr_entries, uint64_t default_value) {
			uint64_traits::ref_counter rc;

			a_.reset(new array64(tm_, rc));

			if (nr_entries)
				a_->grow(nr_entries, default_value);
		}

		void
		reopen_array() {
			uint64_traits::ref_counter rc;
			unsigned nr_entries = a_->get_nr_entries();
			block_address root = a_->get_root();
			a_.reset(new array64(tm_, rc, root, nr_entries));
		}

		void
		reopen_array(unsigned nr_entries) {
			uint64_traits::ref_counter rc;
			block_address root = a_->get_root();
			a_.reset(new array64(tm_, rc, root, nr_entries));
		}

		uint64_t get(unsigned index) const {
			return a_->get(index);
		}

		void set(unsigned index, uint64_t const &value) {
			a_->set(index, value);
		}

		array64::ptr a_;

	private:
		block_manager<>::ptr bm_;
		space_map::ptr sm_;
		transaction_manager tm_;
	};

	class value_visitor {
	public:
		void visit(unsigned index, uint64_t value) {
			m.insert(make_pair(index, value));
		}

		std::map<unsigned, uint64_t> m;
	};

	class damage_visitor {
	public:
		void visit(array_detail::damage const &d) {
			ds.push_back(d);
		}

		std::list<array_detail::damage> ds;
	};
}

//----------------------------------------------------------------

TEST_F(ArrayTests, can_create_an_empty_array)
{
	create_array(0, 0);
	ASSERT_THROW(get(0), runtime_error);
}

TEST_F(ArrayTests, get_elements)
{
	unsigned const COUNT = 10000;
	create_array(COUNT, 123);

	for (unsigned i = 0; i < COUNT; i++)
		ASSERT_THAT(get(i), Eq(123u));

	ASSERT_THROW(get(COUNT), runtime_error);
}

TEST_F(ArrayTests, set_elements)
{
	unsigned const COUNT = 10000;
	create_array(COUNT, 123);

	for (unsigned i = 0; i < COUNT; i++)
		set(i, 124);

	for (unsigned i = 0; i < COUNT; i++)
		ASSERT_THAT(get(i), Eq(124u));

	ASSERT_THROW(get(COUNT), runtime_error);
}

template <typename T, unsigned size>
unsigned array_size(T (&)[size]) {
	return size;
}

TEST_F(ArrayTests, grow)
{
	unsigned const COUNT = 10000;
	unsigned const STEPS[] = {
		17, 71, 137, 277, 439, 683, 967
	};

	for (unsigned s = 0; s < array_size(STEPS); s++) {

		unsigned step = STEPS[s];

		vector<unsigned> chunks;
		for (unsigned c = 0; c < COUNT; c += step)
			chunks.push_back(c);
		chunks.push_back(COUNT);

		create_array(0, 123);

		for (unsigned i = 1; i < chunks.size(); i++) {
			if (i > 1)
				ASSERT_THAT(get(chunks[i - 1] - 1), Eq(i - 1));

			a_->grow(chunks[i], i);

			if (i > 1)
				ASSERT_THAT(get(chunks[i - 1] - 1), Eq(i - 1));

			for (unsigned j = chunks[i - 1]; j < chunks[i]; j++)
				ASSERT_THAT(get(j), Eq(i));

			ASSERT_THROW(get(chunks[i] + 1), runtime_error);
		}
	}
}

TEST_F(ArrayTests, reopen_array)
{
	unsigned const COUNT = 10000;

	create_array(COUNT, 123);
	for (unsigned i = 0; i < COUNT; i += 7)
		set(i, 234);

	reopen_array();
	for (unsigned i = 0; i < COUNT; i++)
		ASSERT_THAT(get(i), Eq(i % 7 ? 123u : 234u));
}

TEST_F(ArrayTests, visit_values)
{
	unsigned const COUNT = 10000;

	create_array(COUNT, 123);

	value_visitor vv;
	damage_visitor dv;
	a_->visit_values(vv, dv);

	for (unsigned i = 0; i < COUNT; i++) {
		unsigned c = vv.m.count(i);
		uint64_t v = vv.m[i];
		ASSERT_THAT(c, Eq(1u));
		ASSERT_THAT(v, Eq(123ull));
	}
}

TEST_F(ArrayTests, visit_values_with_too_few_entries)
{
	unsigned const COUNT = 10000;
	unsigned const EXTRA = 2 * COUNT;

	create_array(COUNT, 123);
	reopen_array(EXTRA);

	value_visitor vv;
	damage_visitor dv;
	a_->visit_values(vv, dv);

	for (unsigned i = 0; i < COUNT; i++) {
		unsigned c = vv.m.count(i);
		uint64_t v = vv.m[i];
		ASSERT_THAT(c, Eq(1u));
		ASSERT_THAT(v, Eq(123ull));
	}

	ASSERT_THAT(dv.ds.size(), Eq(1ul));
	ASSERT_THAT(dv.ds.front().lost_keys_, Eq(run<uint32_t>(COUNT, EXTRA)));
}

//----------------------------------------------------------------

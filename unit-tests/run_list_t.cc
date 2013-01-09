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

#include "persistent-data/run_list.h"

#define BOOST_TEST_MODULE RunListTests
#include <boost/test/included/unit_test.hpp>

using namespace std;
using namespace boost;
using namespace base;

//----------------------------------------------------------------

namespace {
	void check_run(run_list<unsigned> const &rl,
		       unsigned b, unsigned e, bool in) {
		for (unsigned i = b; i < e; i++)
			BOOST_CHECK(rl.in_run(i) == in);
	}

	bool id(bool b) {
		return b;
	}

	bool negate(bool b) {
		return !b;
	}

	void _test_added_runs_are_recorded(bool invert) {
		run_list<unsigned> rl;
		rl.add_run(10, 87);

		if (invert)
			rl.invert();
		bool (*op)(bool) = invert ? negate : id;

		check_run(rl, 0, 10, op(false));
		check_run(rl, 10, 87, op(true));
		check_run(rl, 87, 100, op(false));
	}

	void _test_multiple_runs_are_recorded(bool invert) {
		run_list<unsigned> rl;

		rl.add_run(10, 87);
		rl.add_run(100, 111);
		rl.add_run(678, 789);

		if (invert)
			rl.invert();
		bool (*op)(bool) = invert ? negate : id;

		check_run(rl, 0, 10, op(false));
		check_run(rl, 10, 87, op(true));
		check_run(rl, 87, 100, op(false));
		check_run(rl, 100, 111, op(true));
		check_run(rl, 111, 678, op(false));
		check_run(rl, 678, 789, op(true));
		check_run(rl, 789, 1000, op(false));
	}

	void _test_overlap_tail(bool invert) {
		run_list<unsigned> rl;

		rl.add_run(10, 87);
		rl.add_run(50, 96);

		if (invert)
			rl.invert();
		bool (*op)(bool) = invert ? negate : id;

		check_run(rl, 0, 10, op(false));
		check_run(rl, 10, 96, op(true));
		check_run(rl, 96, 100, op(false));
	}

	void _test_overlap_head(bool invert) {
		run_list<unsigned> rl;

		rl.add_run(50, 96);
		rl.add_run(10, 87);

		if (invert)
			rl.invert();
		bool (*op)(bool) = invert ? negate : id;

		check_run(rl, 0, 10, op(false));
		check_run(rl, 10, 96, op(true));
		check_run(rl, 96, 100, op(false));
	}

	void _test_overlap_completely(bool invert) {
		run_list<unsigned> rl;

		rl.add_run(40, 60);
		rl.add_run(10, 87);

		if (invert)
			rl.invert();
		bool (*op)(bool) = invert ? negate : id;

		check_run(rl, 0, 10, op(false));
		check_run(rl, 10, 87, op(true));
		check_run(rl, 87, 100, op(false));
	}

	void _test_overlap_many(bool invert) {
		run_list<unsigned> rl;

		rl.add_run(20, 30);
		rl.add_run(35, 40);
		rl.add_run(45, 50);
		rl.add_run(55, 60);
		rl.add_run(10, 87);

		if (invert)
			rl.invert();
		bool (*op)(bool) = invert ? negate : id;

		check_run(rl, 0, 10, op(false));
		check_run(rl, 10, 87, op(true));
		check_run(rl, 87, 100, op(false));
	}
}

//----------------------------------------------------------------

BOOST_AUTO_TEST_CASE(test_added_runs_are_recorded)
{
	_test_added_runs_are_recorded(false);
}

BOOST_AUTO_TEST_CASE(test_multiple_runs_are_recorded)
{
	_test_multiple_runs_are_recorded(false);
}

BOOST_AUTO_TEST_CASE(test_overlap_tail)
{
	_test_overlap_tail(false);
}

BOOST_AUTO_TEST_CASE(test_overlap_head)
{
	_test_overlap_head(false);
}

BOOST_AUTO_TEST_CASE(test_overlap_completely)
{
	_test_overlap_completely(false);
}

BOOST_AUTO_TEST_CASE(test_overlap_many)
{
	_test_overlap_many(false);
}

BOOST_AUTO_TEST_CASE(test_added_runs_are_recorded_inverted)
{
	_test_added_runs_are_recorded(true);
}

BOOST_AUTO_TEST_CASE(test_multiple_runs_are_recorded_inverted)
{
	_test_multiple_runs_are_recorded(true);
}

BOOST_AUTO_TEST_CASE(test_overlap_tail_inverted)
{
	_test_overlap_tail(true);
}

BOOST_AUTO_TEST_CASE(test_overlap_head_inverted)
{
	_test_overlap_head(true);
}

BOOST_AUTO_TEST_CASE(test_overlap_completely_inverted)
{
	_test_overlap_completely(true);
}

BOOST_AUTO_TEST_CASE(test_overlap_many_inverted)
{
	_test_overlap_many(true);
}

//----------------------------------------------------------------

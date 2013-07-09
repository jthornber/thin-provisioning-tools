#include "gmock/gmock.h"

#include "persistent-data/run_set.h"

#include <memory>

using namespace base;
using namespace std;
using namespace testing;

//----------------------------------------------------------------

namespace {
	MATCHER_P2(EqRun, b, e, "") {
		return (arg.begin_ == static_cast<unsigned>(b)) &&
			(arg.end_ == static_cast<unsigned>(e));
	}

	MATCHER(EqAll, "") {
		return !arg.begin_ && !arg.end_;
	}

	MATCHER_P(EqOpenBegin, e, "") {
		return !arg.begin_ && (arg.end_ == static_cast<unsigned>(e));
	}

	MATCHER_P(EqOpenEnd, b, "") {
		return (arg.begin_ == static_cast<unsigned>(b)) && !arg.end_;
	}

	class RunSetTests : public Test {
	};
}

//----------------------------------------------------------------

TEST_F(RunSetTests, create)
{
	auto_ptr<run_set<unsigned> > rs(new run_set<unsigned>());
}

TEST_F(RunSetTests, add_single_blocks)
{
	run_set<unsigned> rs;

	rs.add(3u);
	rs.add(8u);
	rs.add(9u);
}

TEST_F(RunSetTests, add_adjacent_single)
{
	run_set<unsigned> rs;

	rs.add(3);
	rs.add(4);

	ASSERT_THAT(*rs.begin(), EqRun(3, 5));
	ASSERT_THAT(++rs.begin(), Eq(rs.end()));
}

TEST_F(RunSetTests, add_adjacent_single_other_way_round)
{
	run_set<unsigned> rs;

	rs.add(4);
	rs.add(3);

	ASSERT_THAT(*rs.begin(), EqRun(3, 5));
	ASSERT_THAT(++rs.begin(), Eq(rs.end()));
}

TEST_F(RunSetTests, many_single_blocks)
{
	run_set<unsigned> rs;

	for (unsigned i = 1; i < 100000; i++)
		rs.add(i);

	ASSERT_THAT(*rs.begin(), EqRun(1, 100000));
	ASSERT_THAT(++rs.begin(), Eq(rs.end()));
}

TEST_F(RunSetTests, add_runs)
{
	run_set<unsigned> rs;

	rs.add(run<unsigned>(3, 8));
	rs.add(run<unsigned>(23, 55));
	rs.add(run<unsigned>(78, 190));
}

TEST_F(RunSetTests, member_empty_set)
{
	run_set<unsigned> rs;
	ASSERT_THAT(rs.begin(), Eq(rs.end()));
	ASSERT_FALSE(rs.member(5));
}

TEST_F(RunSetTests, member_many_runs)
{
	run_set<unsigned> rs;

	rs.add(3);
	rs.add(7, 15);
	rs.add(23, 28);
	rs.add(25, 50);

	rs.add(101, 105);
	rs.add(105, 110);

	rs.add(201, 205);
	rs.add(206, 210);

	ASSERT_TRUE(rs.member(3));

	ASSERT_FALSE(rs.member(6));
	for (unsigned i = 7; i < 15; i++)
		ASSERT_TRUE(rs.member(i));
	ASSERT_FALSE(rs.member(15));

	ASSERT_FALSE(rs.member(22));
	for (unsigned i = 23; i < 50; i++)
		ASSERT_TRUE(rs.member(i));
	ASSERT_FALSE(rs.member(50));

	ASSERT_FALSE(rs.member(100));
	for (unsigned i = 101; i < 110; i++)
		ASSERT_TRUE(rs.member(i));
	ASSERT_FALSE(rs.member(110));

	ASSERT_FALSE(rs.member(200));
	for (unsigned i = 201; i < 205; i++)
		ASSERT_TRUE(rs.member(i));
	ASSERT_FALSE(rs.member(205));
	for (unsigned i = 206; i < 210; i++)
		ASSERT_TRUE(rs.member(i));
	ASSERT_FALSE(rs.member(210));
}

TEST_F(RunSetTests, iterate_empty)
{
	run_set<unsigned> rs;
	ASSERT_THAT(rs.begin(), Eq(rs.end()));
}

TEST_F(RunSetTests, iterate_single)
{
	run_set<unsigned> rs;
	rs.add(5, 10);
	ASSERT_THAT(*rs.begin(), EqRun(5, 10));
	ASSERT_THAT(++rs.begin(), Eq(rs.end()));
}

TEST_F(RunSetTests, iterate_overlapping_runs)
{
	run_set<unsigned> rs;
	rs.add(5, 10);
	rs.add(8, 15);
	ASSERT_THAT(*rs.begin(), EqRun(5, 15));
	ASSERT_THAT(++rs.begin(), Eq(rs.end()));
}

TEST_F(RunSetTests, iterate_overlapping_runs_other_way)
{
	run_set<unsigned> rs;
	rs.add(8, 15);
	rs.add(5, 10);
	ASSERT_THAT(*rs.begin(), EqRun(5, 15));
	ASSERT_THAT(++rs.begin(), Eq(rs.end()));
}

TEST_F(RunSetTests, iterate_covered_runs)
{
	run_set<unsigned> rs;
	rs.add(8, 15);
	rs.add(5, 20);
	ASSERT_THAT(*rs.begin(), EqRun(5, 20));
	ASSERT_THAT(++rs.begin(), Eq(rs.end()));
}

TEST_F(RunSetTests, iterate_covered_runs2)
{
	run_set<unsigned> rs;
	rs.add(8, 10);
	rs.add(12, 20);
	rs.add(5, 30);
	ASSERT_THAT(*rs.begin(), EqRun(5, 30));
	ASSERT_THAT(++rs.begin(), Eq(rs.end()));
}

TEST_F(RunSetTests, iterate_non_overlapping_runs)
{
	run_set<unsigned> rs;
	rs.add(5, 10);
	rs.add(15, 20);
	ASSERT_THAT(*rs.begin(), EqRun(5, 10));
	ASSERT_THAT(*(++rs.begin()), EqRun(15, 20));
	ASSERT_THAT(++(++(rs.begin())), Eq(rs.end()));
}

TEST_F(RunSetTests, merge_empty_sets)
{
	run_set<unsigned> rs1;
	run_set<unsigned> rs2;

	rs1.merge(rs2);
	ASSERT_THAT(rs1.begin(), Eq(rs1.end()));
}

TEST_F(RunSetTests, merge_discrete_sets)
{
	run_set<unsigned> rs1;
	run_set<unsigned> rs2;

	rs1.add(5, 10);
	rs2.add(15, 20);
	rs1.merge(rs2);
	ASSERT_THAT(*rs1.begin(), EqRun(5, 10));
	ASSERT_THAT(*(++rs1.begin()), EqRun(15, 20));
	ASSERT_THAT(++(++(rs1.begin())), Eq(rs1.end()));
}

TEST_F(RunSetTests, negate_empty)
{
	run_set<unsigned> rs;
	rs.negate();
	ASSERT_THAT(*rs.begin(), EqAll());
	ASSERT_THAT(++rs.begin(), Eq(rs.end()));
}

TEST_F(RunSetTests, negate_single)
{
	run_set<unsigned> rs;
	rs.add(5, 10);
	rs.negate();
	ASSERT_THAT(*rs.begin(), EqOpenBegin(5));
	ASSERT_THAT(*(++rs.begin()), EqOpenEnd(10));
	ASSERT_THAT(++(++rs.begin()), Eq(rs.end()));
}

TEST_F(RunSetTests, negate_single2)
{
	run_set<unsigned> rs;
	rs.add(23);
	rs.negate();
	ASSERT_THAT(*rs.begin(), EqOpenBegin(23));
	ASSERT_THAT(*(++rs.begin()), EqOpenEnd(24));
	ASSERT_THAT(++(++rs.begin()), Eq(rs.end()));
}

TEST_F(RunSetTests, negate_double)
{
	run_set<unsigned> rs;
	rs.add(5, 10);
	rs.add(15, 20);
	rs.negate();
	ASSERT_THAT(*rs.begin(), EqOpenBegin(5));
	ASSERT_THAT(*(++rs.begin()), EqRun(10, 15));
	ASSERT_THAT(*(++(++rs.begin())), EqOpenEnd(20));
	ASSERT_THAT(++(++(++rs.begin())), Eq(rs.end()));
}

TEST_F(RunSetTests, negate_negate)
{
	run_set<unsigned> rs;
	rs.add(5, 10);
	rs.add(15, 20);
	rs.negate();
	rs.negate();
	ASSERT_THAT(*rs.begin(), EqRun(5, 10));
	ASSERT_THAT(*(++rs.begin()), EqRun(15, 20));
	ASSERT_THAT(++(++rs.begin()), Eq(rs.end()));
}

//----------------------------------------------------------------

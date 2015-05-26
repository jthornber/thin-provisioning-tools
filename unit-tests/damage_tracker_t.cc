#include "gmock/gmock.h"

#include "test_utils.h"

#include "persistent-data/data-structures/btree_damage_visitor.h"

using namespace std;
using namespace persistent_data;
using namespace test;
using namespace testing;

//----------------------------------------------------------------

namespace {
	typedef run<block_address> run64;
	typedef damage_tracker::maybe_run64 mr64;

	class DamageTrackerTests : public Test {
	public:
		void assert_no_damage(mr64 const &mr) const {
			ASSERT_THAT(mr, Eq(mr64()));
		}

		void assert_damage(mr64 const &mr, run64 const &expected) const {
			ASSERT_THAT(mr, Eq(mr64(expected)));
		}

		damage_tracker dt;
	};
};

//----------------------------------------------------------------

TEST_F(DamageTrackerTests, good_leaf)
{
	assert_no_damage(dt.good_leaf(0, 10));
	assert_no_damage(dt.end());
}

TEST_F(DamageTrackerTests, bad_node)
{
	dt.bad_node();
	assert_damage(dt.end(), run64(0ull));
}

TEST_F(DamageTrackerTests, good_bad)
{
	dt.good_leaf(0, 10);
	dt.bad_node();
	assert_damage(dt.end(), run64(10ull));
}

TEST_F(DamageTrackerTests, bad_good)
{
	dt.bad_node();
	assert_damage(dt.good_leaf(10, 20), run64(0ull, 10ull));
}

TEST_F(DamageTrackerTests, good_bad_good)
{
	dt.good_leaf(0, 10);
	dt.bad_node();
	assert_damage(dt.good_leaf(20, 30), run64(10ull, 20ull));
	assert_no_damage(dt.end());
}

TEST_F(DamageTrackerTests, bad_good_bad)
{
	dt.bad_node();
	dt.good_leaf(10, 20);
	dt.bad_node();
	assert_damage(dt.end(), run64(20ull));
}

TEST_F(DamageTrackerTests, gi_bl_gl)
{
	dt.good_internal(0);
	dt.bad_node();
	assert_damage(dt.good_leaf(10, 20), run64(0ull, 10ull));
	assert_no_damage(dt.end());
}

TEST_F(DamageTrackerTests, gi_gl_bl_bi)
{
	dt.good_internal(0);
	dt.good_leaf(0, 10);
	dt.bad_node();
	dt.bad_node();
	assert_damage(dt.end(), run64(10ull));
}

TEST_F(DamageTrackerTests, gi_bi_gi_bl_gl)
{
	dt.good_internal(0);
	dt.bad_node();
	assert_damage(dt.good_internal(10), run64(0ull, 10ull));
	dt.bad_node();
	assert_damage(dt.good_leaf(15, 20), run64(10ull, 15ull));
}

TEST_F(DamageTrackerTests, end_resets_tracker)
{
	dt.good_internal(0);
	dt.good_leaf(0, 10);
	dt.bad_node();
	assert_damage(dt.end(), run64(10ull));

	assert_no_damage(dt.good_leaf(20, 30));
	assert_no_damage(dt.end());
}

//----------------------------------------------------------------

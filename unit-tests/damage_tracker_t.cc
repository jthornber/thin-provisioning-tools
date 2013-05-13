#include "gmock/gmock.h"

#include "test_utils.h"

#include "persistent-data/data-structures/btree_damage_visitor.h"

using namespace std;
using namespace persistent_data;
using namespace test;
using namespace testing;

//----------------------------------------------------------------

namespace {
	typedef range<block_address> range64;
	typedef damage_tracker::maybe_range64 mr64;

	class DamageTrackerTests : public Test {
	public:
		void assert_no_damage(mr64 const &mr) const {
			ASSERT_THAT(mr, Eq(mr64()));
		}

		void assert_damage(mr64 const &mr, range64 const &expected) const {
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
	assert_damage(dt.end(), range64(0ull));
}

TEST_F(DamageTrackerTests, good_bad)
{
	dt.good_leaf(0, 10);
	dt.bad_node();
	assert_damage(dt.end(), range64(10ull));
}

TEST_F(DamageTrackerTests, bad_good)
{
	dt.bad_node();
	assert_damage(dt.good_leaf(10, 20), range64(0ull, 10ull));
}

TEST_F(DamageTrackerTests, good_bad_good)
{
	dt.good_leaf(0, 10);
	dt.bad_node();
	assert_damage(dt.good_leaf(20, 30), range64(10ull, 20ull));
	assert_no_damage(dt.end());
}

TEST_F(DamageTrackerTests, bad_good_bad)
{
	dt.bad_node();
	dt.good_leaf(10, 20);
	dt.bad_node();
	assert_damage(dt.end(), range64(20ull));
}

TEST_F(DamageTrackerTests, gi_bl_gl)
{
	dt.good_internal(0);
	dt.bad_node();
	assert_damage(dt.good_leaf(10, 20), range64(0ull, 10ull));
	assert_no_damage(dt.end());
}

TEST_F(DamageTrackerTests, gi_gl_bl_bi)
{
	dt.good_internal(0);
	dt.good_leaf(0, 10);
	dt.bad_node();
	dt.bad_node();
	assert_damage(dt.end(), range64(10ull));
}

TEST_F(DamageTrackerTests, gi_bi_gi_bl_gl)
{
	dt.good_internal(0);
	dt.bad_node();
	assert_damage(dt.good_internal(10), range64(0ull, 10ull));
	dt.bad_node();
	assert_damage(dt.good_leaf(15, 20), range64(10ull, 15ull));
}

//----------------------------------------------------------------

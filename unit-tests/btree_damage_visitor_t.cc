#include "gmock/gmock.h"

#include "test_utils.h"

#include "persistent-data/data-structures/btree_damage_visitor.h"
#include "persistent-data/endian_utils.h"
#include "persistent-data/space-maps/core.h"
#include "persistent-data/transaction_manager.h"

using namespace std;
using namespace persistent_data;
using namespace test;
using namespace testing;

//----------------------------------------------------------------

namespace {
	block_address const BLOCK_SIZE = 4096;
	block_address const NR_BLOCKS = 102400;

	struct thing {
		uint32_t x;
		uint64_t y;
	};

	struct thing_disk {
		le32 x;
		le64 y;
	};

	struct thing_traits {
		typedef thing_disk disk_type;
		typedef thing value_type;
		typedef persistent_data::no_op_ref_counter<value_type> ref_counter;

		static void unpack(thing_disk const &disk, thing &value) {
			value.x = to_cpu<uint32_t>(disk.x);
			value.y = to_cpu<uint64_t>(disk.y);
		}

		static void pack(thing const &value, thing_disk &disk) {
			disk.x = to_disk<le32>(value.x);
			disk.y = to_disk<le64>(value.y);
		}
	};

	class value_visitor_mock {
	public:
		MOCK_METHOD1(visit, void(thing const &));
	};

	class damage_visitor_mock {
	public:
		MOCK_METHOD1(visit, void(btree_detail::damage const &));
	};

	class BTreeDamageVisitorTests : public Test {
	public:
		BTreeDamageVisitorTests()
			: bm_(create_bm<BLOCK_SIZE>(NR_BLOCKS)),
			  sm_(new core_map(NR_BLOCKS)),
			  tm_(new transaction_manager(bm_, sm_)),
			  tree_(new btree<2, thing_traits>(tm_, rc_)) {
		}

		void zero_block(block_address b) {
			::test::zero_block(bm_, b);
		}

		with_temp_directory dir_;
		block_manager<>::ptr bm_;
		space_map::ptr sm_;
		transaction_manager::ptr tm_;

		thing_traits::ref_counter rc_;
		btree<2, thing_traits>::ptr tree_;

		value_visitor_mock value_visitor_;
		damage_visitor_mock damage_visitor_;
	};
}

//----------------------------------------------------------------

TEST_F(BTreeDamageVisitorTests, visiting_an_empty_btree_visits_nothing)
{
	block_counter counter;

	EXPECT_CALL(value_visitor_, visit(_)).Times(0);
	EXPECT_CALL(damage_visitor_, visit(_)).Times(0);

	btree_damage_visitor<value_visitor_mock, damage_visitor_mock, 2, thing_traits>
		visitor(counter, value_visitor_, damage_visitor_);
	tree_->visit_depth_first(visitor);
}

TEST_F(BTreeDamageVisitorTests, visiting_an_tree_with_a_trashed_root)
{
	zero_block(tree_->get_root());
	EXPECT_CALL(value_visitor_, visit(_)).Times(0);
	EXPECT_CALL(damage_visitor_, visit(Eq(damage(0, range<uint64_t>(), "foo")))).Times(1);

	block_counter counter;

	btree_damage_visitor<value_visitor_mock, damage_visitor_mock, 2, thing_traits>
		visitor(counter, value_visitor_, damage_visitor_);
	tree_->visit_depth_first(visitor);
}

#if 0
	uint64_t key[2] = {1, 2};
	thing value = {0, 0};
	tree_->insert(key, value);
#endif

//----------------------------------------------------------------

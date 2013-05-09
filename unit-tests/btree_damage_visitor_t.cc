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
	block_address const SUPERBLOCK = 0;

	struct thing {
		thing(/* uint32_t x_ = 0, */ uint64_t y_ = 0)
			: /* x(x_), */
			  y(y_) {
		}

		bool operator ==(thing const &rhs) const {
			return /* (x == rhs.x) && */ (y == rhs.y);
		}

		//uint32_t x;
		uint64_t y;
	};

	struct thing_disk {
		//le32 x;
		le64 y;
	};

	struct thing_traits {
		typedef thing_disk disk_type;
		typedef thing value_type;
		typedef persistent_data::no_op_ref_counter<value_type> ref_counter;

		static void unpack(thing_disk const &disk, thing &value) {
			//	value.x = to_cpu<uint32_t>(disk.x);
			value.y = to_cpu<uint64_t>(disk.y);
		}

		static void pack(thing const &value, thing_disk &disk) {
			//	disk.x = to_disk<le32>(value.x);
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
			  sm_(setup_core_map()),
			  tm_(new transaction_manager(bm_, sm_)),
			  tree_(new btree<1, thing_traits>(tm_, rc_)) {
		}

		space_map::ptr setup_core_map() {
			space_map::ptr sm(new core_map(NR_BLOCKS));
			sm->inc(SUPERBLOCK);
			return sm;
		}

		void commit() {
			block_manager<>::write_ref superblock(bm_->superblock(SUPERBLOCK));
		}

		void trash_block(block_address b) {
			::test::zero_block(bm_, b);
		}

		void insert_values(unsigned nr) {
			for (unsigned i = 0; i < nr; i++) {
				uint64_t key[1] = {i};
				thing value(i /*, i + 1234 */);

				tree_->insert(key, value);
			}
		}

		void expect_no_values() {
			EXPECT_CALL(value_visitor_, visit(_)).Times(0);
		}

		void expect_nr_values(unsigned nr) {
			for (unsigned i = 0; i < nr; i++)
				EXPECT_CALL(value_visitor_, visit(Eq(thing(i /*, i + 1234 */)))).Times(1);
		}

		void expect_value(unsigned n) {
			EXPECT_CALL(value_visitor_, visit(Eq(thing(n /*, n + 1234 */)))).Times(1);
		}

		void expect_no_damage() {
			EXPECT_CALL(damage_visitor_, visit(_)).Times(0);
		}

		void expect_damage(unsigned level, range<uint64_t> keys) {
			EXPECT_CALL(damage_visitor_, visit(Eq(damage(level, keys, "foo")))).Times(1);
		}

		void run() {
			// We must commit before we do the test to ensure
			// all the block numbers and checksums are written
			// to the btree nodes.
			commit();

			block_counter counter;
			btree_damage_visitor<value_visitor_mock, damage_visitor_mock, 1, thing_traits>
				visitor(counter, value_visitor_, damage_visitor_);
			tree_->visit_depth_first(visitor);
		}

		with_temp_directory dir_;
		block_manager<>::ptr bm_;
		space_map::ptr sm_;
		transaction_manager::ptr tm_;

		thing_traits::ref_counter rc_;
		btree<1, thing_traits>::ptr tree_;

		value_visitor_mock value_visitor_;
		damage_visitor_mock damage_visitor_;
	};
}

//----------------------------------------------------------------

TEST_F(BTreeDamageVisitorTests, visiting_an_empty_tree)
{
	expect_no_values();
	expect_no_damage();

	run();
}

TEST_F(BTreeDamageVisitorTests, visiting_an_tree_with_a_trashed_root)
{
	trash_block(tree_->get_root());

	expect_no_values();
	expect_damage(0, range<uint64_t>());

	run();
}

TEST_F(BTreeDamageVisitorTests, visiting_a_populated_tree_with_no_damage)
{
	insert_values(1000);

	expect_nr_values(1000);
	expect_no_damage();

	run();
}

//----------------------------------------------------------------

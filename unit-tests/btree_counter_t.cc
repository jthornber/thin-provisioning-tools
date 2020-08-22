#include "gmock/gmock.h"

#include "test_utils.h"

#include "persistent-data/data-structures/btree.h"
#include "persistent-data/data-structures/btree_counter.h"
#include "persistent-data/space-maps/core.h"
#include "persistent-data/data-structures/simple_traits.h"

using namespace base;
using namespace std;
using namespace persistent_data;
using namespace test;
using namespace testing;

//----------------------------------------------------------------

namespace {
	block_address const BLOCK_SIZE = 4096;
	block_address const NR_BLOCKS = 102400;
	block_address const SUPERBLOCK = 0;

	class BTreeCounterTests : public Test {
	public:
		BTreeCounterTests()
			: bm_(create_bm(NR_BLOCKS)),
			  sm_(setup_core_map()),
			  tm_(bm_, sm_) {
		}

		size_t get_nr_metadata_blocks(bool ignore_non_fatal = false,
					      bool stop_on_error = false) {
			block_counter bc(stop_on_error);
			noop_value_counter<uint64_t> vc;
			count_btree_blocks(*tree_, bc, vc, ignore_non_fatal);
			return bc.get_counts().size();
		}

		void damage_first_leaf_underfull() {
			bcache::validator::ptr v = create_btree_node_validator();

			block_address b = get_first_leaf();
			block_manager::write_ref blk = bm_->write_lock(b, v);
			btree<1, uint64_traits>::leaf_node n = to_node<uint64_traits>(blk);
			n.set_nr_entries(1);
		}

		with_temp_directory dir_;
		block_manager::ptr bm_;
		space_map::ptr sm_;
		transaction_manager tm_;
		uint64_traits::ref_counter rc_;

		btree<1, uint64_traits>::ptr tree_;

	private:
		space_map::ptr setup_core_map() {
			space_map::ptr sm(create_core_map(NR_BLOCKS));
			sm->inc(SUPERBLOCK);
			return sm;
		}

		void commit() {
			block_manager::write_ref superblock(bm_->superblock(SUPERBLOCK));
		}

		block_address get_first_leaf() {
			bcache::validator::ptr v = create_btree_node_validator();

			block_manager::read_ref root = bm_->read_lock(tree_->get_root(), v);
			btree<1, uint64_traits>::internal_node n = to_node<block_traits>(root);
			while (n.get_type() == INTERNAL) {
				block_manager::read_ref internal = bm_->read_lock(n.value_at(0), v);
				n = to_node<block_traits>(internal);
			}

			return n.get_block_nr();
		}
	};
}

//----------------------------------------------------------------

TEST_F(BTreeCounterTests, count_empty_tree)
{
	tree_.reset(new btree<1, uint64_traits>(tm_, rc_));
	tm_.get_bm()->flush();
	ASSERT_GE(get_nr_metadata_blocks(), 1u);
}

TEST_F(BTreeCounterTests, count_populated_tree)
{
	tree_.reset(new btree<1, uint64_traits>(tm_, rc_));

	for (unsigned i = 0; i < 10000; i++) {
		uint64_t key[1] = {i};
		tree_->insert(key, 0ull);
	}

	tm_.get_bm()->flush();
	ASSERT_GE(get_nr_metadata_blocks(), 40u);
}

TEST_F(BTreeCounterTests, count_underfull_nodes)
{
	tree_.reset(new btree<1, uint64_traits>(tm_, rc_));

	for (unsigned i = 0; i < 10000; i++) {
		uint64_t key[1] = {i};
		tree_->insert(key, 0ull);
	}

	tm_.get_bm()->flush();
	size_t nr_blocks = get_nr_metadata_blocks();

	damage_first_leaf_underfull();
	tm_.get_bm()->flush();

	// underfull nodes are not counted
	bool ignore_non_fatal = false;
	bool stop_on_error = false;
	ASSERT_EQ(get_nr_metadata_blocks(ignore_non_fatal, stop_on_error), nr_blocks - 1);

	// underfull nodes are counted
	ignore_non_fatal = true;
	stop_on_error = false;
	ASSERT_EQ(get_nr_metadata_blocks(ignore_non_fatal, stop_on_error), nr_blocks);

	// logical errors like underfull nodes don't result in exceptions,
	// therefore the stop_on_error flag has no effect.
	// FIXME: is it necessary to stop the counting on logical errors?
	ignore_non_fatal = false;
	stop_on_error = true;
	ASSERT_EQ(get_nr_metadata_blocks(ignore_non_fatal, stop_on_error), nr_blocks - 1);

	ignore_non_fatal = true;
	stop_on_error = true;
	ASSERT_EQ(get_nr_metadata_blocks(ignore_non_fatal, stop_on_error), nr_blocks);
}

//----------------------------------------------------------------

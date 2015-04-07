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
			: bm_(create_bm<BLOCK_SIZE>(NR_BLOCKS)),
			  sm_(setup_core_map()),
			  tm_(bm_, sm_) {
		}

		void check_nr_metadata_blocks_is_ge(unsigned n) {
			block_counter bc;
			noop_value_counter<uint64_t> vc;
			count_btree_blocks(*tree_, bc, vc);
			ASSERT_THAT(bc.get_counts().size(), Ge(n));
		}

		with_temp_directory dir_;
		block_manager<>::ptr bm_;
		space_map::ptr sm_;
		transaction_manager tm_;
		uint64_traits::ref_counter rc_;

		btree<1, uint64_traits>::ptr tree_;

	private:
		space_map::ptr setup_core_map() {
			space_map::ptr sm(new core_map(NR_BLOCKS));
			sm->inc(SUPERBLOCK);
			return sm;
		}

		void commit() {
			block_manager<>::write_ref superblock(bm_->superblock(SUPERBLOCK));
		}
	};

	void dump_counts(block_counter const &bc) {
		block_counter::count_map::const_iterator it, end = bc.get_counts().end();
		for (it = bc.get_counts().begin(); it != end; ++it)
			cout << it->first << " -> " << it->second << endl;
	}
}

//----------------------------------------------------------------

TEST_F(BTreeCounterTests, count_empty_tree)
{
	tree_.reset(new btree<1, uint64_traits>(tm_, rc_));
	check_nr_metadata_blocks_is_ge(1);
}

TEST_F(BTreeCounterTests, count_populated_tree)
{
	tree_.reset(new btree<1, uint64_traits>(tm_, rc_));

	for (unsigned i = 0; i < 10000; i++) {
		uint64_t key[1] = {i};
		tree_->insert(key, 0ull);
	}

	check_nr_metadata_blocks_is_ge(40);
}

//----------------------------------------------------------------

#include "gmock/gmock.h"

#include "test_utils.h"

#include "persistent-data/transaction_manager.h"
#include "persistent-data/space-maps/core.h"
#include "persistent-data/data-structures/btree_damage_visitor.h"

using namespace std;
using namespace persistent_data;
using namespace test;
using namespace testing;

//----------------------------------------------------------------

namespace {
	block_address const BLOCK_SIZE = 4096;
	block_address const NR_BLOCKS = 102400;

	class BTreeDamageVisitorTests : public Test {
	public:
		BTreeDamageVisitorTests()
			: bm_(create_bm<BLOCK_SIZE>(NR_BLOCKS)) {
		}

		with_temp_directory dir_;
		block_manager<>::ptr bm_;
	};
}

//----------------------------------------------------------------

TEST_F(BTreeDamageVisitorTests, null_test)
{
	
}

//----------------------------------------------------------------

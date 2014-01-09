#include "gmock/gmock.h"
#include "base/bits.h"
#include "era/superblock.h"

using namespace base;
using namespace era;
using namespace superblock_damage;
using namespace testing;

//----------------------------------------------------------------

namespace {
	unsigned const NR_METADATA_BLOCKS = 100;

	class damage_visitor_mock : public damage_visitor {
	public:
		MOCK_METHOD1(visit, void (superblock_corrupt const &));
		MOCK_METHOD1(visit, void (superblock_invalid const &));
	};

	class EraSuperblockTests : public Test {
	public:
		EraSuperblockTests() {
			sb_.bloom_tree_root = 1;
			sb_.era_array_root = 2;
		}

		void check() {
			check_superblock(sb_, NR_METADATA_BLOCKS, visitor_);
		}

		void check_invalid() {
			EXPECT_CALL(visitor_, visit(Matcher<superblock_invalid const &>(_))).Times(1);
			check();
		}

		damage_visitor_mock visitor_;
		superblock sb_;
	};
}

//----------------------------------------------------------------

TEST_F(EraSuperblockTests, default_constructed_superblock_is_valid)
{
	check();
}

TEST_F(EraSuperblockTests, clean_shutdown_flag_is_valid)
{
	sb_.flags.set_flag(superblock_flags::CLEAN_SHUTDOWN);
	check();
}

TEST_F(EraSuperblockTests, unhandled_flags_get_set_correctly_and_is_invalid)
{
	uint32_t bad_flag = 1 << 12;
	sb_.flags = superblock_flags(bad_flag | 1);
	ASSERT_THAT(sb_.flags.get_unhandled_flags(), Eq(bad_flag));
	check_invalid();
}

TEST_F(EraSuperblockTests, blocknr_is_in_range)
{
	sb_.blocknr = NR_METADATA_BLOCKS;
	check_invalid();
}

TEST_F(EraSuperblockTests, magic_is_checked)
{
	sb_.magic = 12345;
	check_invalid();
}

TEST_F(EraSuperblockTests, version_gt_1_is_checked)
{
	sb_.version = 2;
	check_invalid();
}

TEST_F(EraSuperblockTests, version_lt_1_is_checked)
{
	sb_.version = 0;
	check_invalid();
}

TEST_F(EraSuperblockTests, metadata_block_size_checked)
{
	sb_.metadata_block_size = 16;
	check_invalid();
}

TEST_F(EraSuperblockTests, bloom_tree_root_isnt_0)
{
	sb_.bloom_tree_root = 0;
	check_invalid();
}

TEST_F(EraSuperblockTests, era_array_root_isnt_0)
{
	sb_.era_array_root = 0;
	check_invalid();
}

TEST_F(EraSuperblockTests, bloom_root_isnt_era_array_root)
{
	sb_.bloom_tree_root = 10;
	sb_.era_array_root = 10;
	check_invalid();
}

//----------------------------------------------------------------

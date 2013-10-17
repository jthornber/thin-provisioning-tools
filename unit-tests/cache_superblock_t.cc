#include "gmock/gmock.h"
#include "base/bits.h"
#include "caching/superblock.h"

using namespace base;
using namespace caching;
using namespace superblock_damage;
using namespace testing;

//----------------------------------------------------------------

namespace {
	class damage_visitor_mock : public damage_visitor {
	public:
		MOCK_METHOD1(visit, void(superblock_corrupt const &));
		MOCK_METHOD1(visit, void(superblock_invalid const &));
	};

	class CacheSuperblockTests : public Test {
	public:
		CacheSuperblockTests() {
		}

		void check() {
			check_superblock(sb_, 100, visitor_);
		}

		void expect_invalid() {
			EXPECT_CALL(visitor_, visit(Matcher<superblock_invalid const &>(_))).Times(1);
		}

		void check_invalid() {
			expect_invalid();
			check();
		}

		damage_visitor_mock visitor_;
		superblock sb_;
	};

	bool operator ==(superblock_corrupt const &lhs, superblock_corrupt const &rhs) {
		return lhs.get_desc() == rhs.get_desc();
	}

	bool operator ==(superblock_invalid const &lhs, superblock_invalid const &rhs) {
		return lhs.get_desc() == rhs.get_desc();
	}

	ostream &operator <<(ostream &out, damage const &d) {
		out << d.get_desc();
		return out;
	}

	ostream &operator <<(ostream &out, superblock_invalid const &d) {
		out << "superblock_invalid: " << d.get_desc();
		return out;
	}
}

//----------------------------------------------------------------

TEST_F(CacheSuperblockTests, default_constructed_superblock_is_valid)
{
	check();
}

TEST_F(CacheSuperblockTests, clean_shutdown_flag_is_valid)
{
	sb_.flags.set_flag(superblock_flags::CLEAN_SHUTDOWN);
	check();
}

TEST_F(CacheSuperblockTests, unhandled_flags_gets_set_correctly_and_is_invalid)
{
	uint32_t bad_flag = 1 << 12;
	sb_.flags = superblock_flags(bad_flag | 1);
	ASSERT_THAT(sb_.flags.get_unhandled_flags(), Eq(bad_flag));
	check_invalid();
}

TEST_F(CacheSuperblockTests, blocknr_is_in_range)
{
	sb_.blocknr = 101;
	check_invalid();
}

TEST_F(CacheSuperblockTests, magic_is_checked)
{
	sb_.magic = 12345;
	check_invalid();
}

TEST_F(CacheSuperblockTests, version_gt_1_is_checked)
{
	sb_.version = 2;
	check_invalid();
}

TEST_F(CacheSuperblockTests, version_lt_1_is_checked)
{
	sb_.version = 0;
	check_invalid();
}

TEST_F(CacheSuperblockTests, policy_name_must_be_null_terminated)
{
	for (unsigned i = 0; i < CACHE_POLICY_NAME_SIZE; i++)
		sb_.policy_name[i] = 'a';

	check_invalid();
}

TEST_F(CacheSuperblockTests, policy_hint_size_checked)
{
	sb_.policy_hint_size = 3;
	check_invalid();

	sb_.policy_hint_size = 129;
	check_invalid();

	sb_.policy_hint_size = 132;
	check_invalid();
}

TEST_F(CacheSuperblockTests, metadata_block_size_checked)
{
	sb_.metadata_block_size = 16;
	check_invalid();
}

TEST_F(CacheSuperblockTests, compat_flags_checked)
{
	sb_.compat_flags = 1;
	check_invalid();
}

TEST_F(CacheSuperblockTests, compat_ro_flags_checked)
{
	sb_.compat_ro_flags = 1;
	check_invalid();
}

TEST_F(CacheSuperblockTests, valid_incompat_flags_are_checked)
{
	set_bit(sb_.incompat_flags, VARIABLE_HINT_SIZE_BIT);
	check();
}

TEST_F(CacheSuperblockTests, invalid_incompat_flags_checked)
{
	set_bit(sb_.incompat_flags, 15);
	check_invalid();
}

//----------------------------------------------------------------

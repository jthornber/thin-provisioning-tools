#include "gmock/gmock.h"
#include "persistent-data/data-structures/bloom_filter.h"
#include "persistent-data/transaction_manager.h"
#include "persistent-data/space-maps/core.h"
#include "persistent-data/data-structures/array_block.h"
#include "test_utils.h"

#include <boost/random/mersenne_twister.hpp>
#include <boost/random/uniform_int_distribution.hpp>
#include <utility>
#include <deque>
#include <vector>
#include <set>

using namespace persistent_data;
using namespace std;
using namespace test;
using namespace testing;

//----------------------------------------------------------------

namespace {
	block_address const BLOCK_SIZE = 4096;
	block_address const NR_BLOCKS = 102400;
	block_address const SUPERBLOCK = 0;

	//--------------------------------

	class BloomFilterTests : public Test {
	public:
		BloomFilterTests()
			: bm_(create_bm<BLOCK_SIZE>(NR_BLOCKS)),
			  sm_(setup_core_map()),
			  tm_(new transaction_manager(bm_, sm_)) {
		}

		set<block_address> generate_random_blocks(unsigned count,
							  block_address max = std::numeric_limits<uint64_t>::max()) {
			set<block_address> r;

			using namespace boost::random;

			mt19937 rng;
			uniform_int_distribution<uint64_t> uniform_dist(0, max);

			while (r.size() < count) {
				block_address b = uniform_dist(rng);
				r.insert(b);
			}

			return r;
		}

		void commit() {
			block_manager<>::write_ref superblock(bm_->superblock(SUPERBLOCK));
		}

		space_map::ptr setup_core_map() {
			space_map::ptr sm(new core_map(NR_BLOCKS));
			sm->inc(SUPERBLOCK);
			return sm;
		}

		with_temp_directory dir_;
		block_manager<>::ptr bm_;
		space_map::ptr sm_;
		transaction_manager::ptr tm_;
	};
}

//----------------------------------------------------------------

TEST_F(BloomFilterTests, nr_bits_must_be_a_power_of_two)
{
	ASSERT_THROW(bloom_filter f(tm_, 1023, 3), runtime_error);
}

TEST_F(BloomFilterTests, can_create_a_bloom_filter)
{
	bloom_filter f(tm_, 1024, 3);
}

TEST_F(BloomFilterTests, no_false_negatives)
{
	bloom_filter f(tm_, 4096, 6);
	set<block_address> bs = generate_random_blocks(1000);

	set<block_address>::const_iterator it;
	for (it = bs.begin(); it != bs.end(); ++it)
		f.set(*it);

	for (it = bs.begin(); it != bs.end(); ++it)
		ASSERT_THAT(f.test(*it), Eq(true));
}

TEST_F(BloomFilterTests, reload_works)
{
	block_address root;
	set<block_address> bs = generate_random_blocks(1000);

	{
		bloom_filter f(tm_, 4096, 6);

		set<block_address>::const_iterator it;
		for (it = bs.begin(); it != bs.end(); ++it)
			f.set(*it);

		f.flush();
		root = f.get_root();
		commit();
	}

	{
		bloom_filter f(tm_, root, 4096, 6);

		set<block_address>::const_iterator it;
		for (it = bs.begin(); it != bs.end(); ++it)
			ASSERT_THAT(f.test(*it), Eq(true));
	}
}

TEST_F(BloomFilterTests, count_false_positives)
{
	block_address nr_blocks = 1024 * 1024;
	block_address written_blocks = nr_blocks / 100;

	unsigned shift = 1;

	while ((1ull << shift) < (16 * written_blocks))
		shift++;
	cerr << "bitset size: " << ((1 << shift) / (8 * 1024)) << "k" << endl;

	bloom_filter f(tm_, 1 << shift, 6);

	set<block_address> bs = generate_random_blocks(written_blocks, nr_blocks);
	set<block_address>::const_iterator it;

	for (it = bs.begin(); it != bs.end(); ++it)
		f.set(*it);

	// f.print_debug(cerr);

	unsigned count = 0;
	for (unsigned i = 0; i < nr_blocks; i++)
		if (!bs.count(i) && f.test(i))
			count++;

	cerr << count << " false positives out of " << nr_blocks << ", "
	     << static_cast<double>(count * 100) / static_cast<double>(nr_blocks)
	     << "%" << endl;
}

//----------------------------------------------------------------

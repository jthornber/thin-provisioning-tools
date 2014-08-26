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
			  tm_(bm_, sm_) {
		}

		set<block_address> generate_random_blocks(unsigned count,
							  block_address max = std::numeric_limits<uint64_t>::max()) {
			set<block_address> r;

			using namespace boost::random;

			uniform_int_distribution<uint64_t> uniform_dist(0, max);

			while (r.size() < count) {
				block_address b = uniform_dist(rng_);
				r.insert(b);
			}

			return r;
		}

		set<block_address> generate_linear_blocks(unsigned count,
							  block_address max = std::numeric_limits<uint64_t>::max()) {
			set<block_address> r;

			for (unsigned i = 0; i < count; i++)
				r.insert(i);

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
		transaction_manager tm_;

		boost::random::mt19937 rng_;
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

unsigned next_power(unsigned n)
{
	unsigned r = 1;
	while (r < n)
		r <<= 1;

	return r;
}

unsigned calc_nr_bits(double false_positive_rate, unsigned dirty_blocks_per_era, unsigned nr_probes)
{
	double k = (double) nr_probes;
	double kth_root = exp(log(false_positive_rate) / k); // can be precomputed

	// FIXME: we need a way to calulate this in kernel?  or should we
	// just pass in the bloom params on the target line?
	double tmp = log(1.0 - kth_root);
	double n = (- k * (double) dirty_blocks_per_era) / tmp;

	return next_power(ceil(n));
}

unsigned calc_m(double fp, unsigned nr_probes, unsigned n)
{
	double k = (double) nr_probes;
	double kth_root = exp(log(fp) / k);
	double tmp = log(1.0 - kth_root);
	double m = (- ((double) n) / k) * tmp;

	return ceil(m);
}

void print_nr_bits_table(double fp, unsigned nr_probes)
{
	cout << "fp = " << fp << ", k = " << nr_probes << endl;

	for (unsigned long long m = 1024; m < (1ull << 25); m *= 2) {
		unsigned n = calc_nr_bits(fp, m, nr_probes);
		unsigned actual_m = calc_m(fp, nr_probes, n);

		cout << "    m = " << m << ", n = " << n << ", " << n / (1024 * 8)
		     << "k, actual_m = " << actual_m << endl;
	}
}

// Not really a test
TEST_F(BloomFilterTests, nr_bits_table)
{
	print_nr_bits_table(0.001, 4);
	print_nr_bits_table(0.001, 6);
	print_nr_bits_table(0.001, 8);
	print_nr_bits_table(0.001, 16);
}

TEST_F(BloomFilterTests, count_false_positives_with_random_inserts)
{
	block_address nr_blocks = 1 << 27;
	block_address written_blocks = nr_blocks / 1024;

	unsigned nr_probes = 6;
	unsigned n = calc_nr_bits(0.001, written_blocks, nr_probes);

	cerr << "bitset size: " << (n / (8 * 1024)) << "k" << endl;

	double ideal_k = log(2) * ((double) n / (double) written_blocks);
	cerr << "Ideal k = " << ideal_k << endl;


	bloom_filter f(tm_, n, nr_probes);

	set<block_address> bs = generate_random_blocks(written_blocks, nr_blocks);
	set<block_address>::const_iterator it;

	for (it = bs.begin(); it != bs.end(); ++it)
		f.set(*it);

	unsigned count = 0;
	for (unsigned i = 0; i < nr_blocks; i++)
		if (!bs.count(i) && f.test(i))
			count++;

	cerr << count << " false positives out of " << nr_blocks << ", "
	     << static_cast<double>(count * 100) / static_cast<double>(nr_blocks)
	     << "%" << endl;
}

TEST_F(BloomFilterTests, count_false_positives_with_linear_inserts)
{
	block_address nr_blocks = 1 << 25;
	block_address written_blocks = nr_blocks / 100;

	double fp = 0.001;
	unsigned nr_probes = 6;
	unsigned n = calc_nr_bits(fp, written_blocks, nr_probes);

	cerr << "bitset size: " << (n / (8 * 1024)) << "k" << endl;

	double ideal_k = log(2) * ((double) n / (double) written_blocks);
	cerr << "Ideal k = " << ideal_k << endl;


	bloom_filter f(tm_, n, nr_probes);

	set<block_address> bs = generate_linear_blocks(written_blocks, nr_blocks);
	set<block_address>::const_iterator it;

	for (it = bs.begin(); it != bs.end(); ++it)
		f.set(*it);

	unsigned count = 0;
	for (unsigned i = 0; i < nr_blocks; i++)
		if (!bs.count(i) && f.test(i))
			count++;

	double actual_fp = static_cast<double>(count) / static_cast<double>(nr_blocks);

	ASSERT_THAT(actual_fp, Lt(fp));

	cerr << count << " false positives out of " << nr_blocks << ", "
	     << actual_fp * 100.0 << "%" << endl;
}

TEST_F(BloomFilterTests, false_positives_over_multiple_eras)
{
	unsigned nr_eras = 10;
	block_address nr_blocks = 1 << 20;
	block_address written_blocks = nr_blocks / nr_eras;

	double fp = 0.001;
	unsigned nr_probes = 6;
	unsigned n = calc_nr_bits(fp, written_blocks, nr_probes);

	cerr << "bitset size: " << (n / (8 * 1024)) << "k" << endl;

	double ideal_k = log(2) * ((double) n / (double) written_blocks);
	cerr << "Ideal k = " << ideal_k << endl;

	vector<set<block_address> > writes(nr_eras);
	vector<bloom_filter::ptr> filters(nr_eras);

	for (unsigned era = 0; era < writes.size(); era++) {
		cerr << "inserting era " << era << endl;

		writes[era] = generate_random_blocks(written_blocks, nr_blocks);
		set<block_address> const &bs = writes[era];

		filters[era] = bloom_filter::ptr(new bloom_filter(tm_, n, nr_probes));
		bloom_filter::ptr &f = filters[era];

		set<block_address>::const_iterator it;
		for (it = bs.begin(); it != bs.end(); ++it)
			f->set(*it);
	}

	set<block_address> write_sum;
	set<block_address> filter_sum;
	for (unsigned era_plus_1 = writes.size(); era_plus_1 > 0; era_plus_1--) {
		unsigned era = era_plus_1 - 1;

		set<block_address> const &era_writes = writes[era];
		write_sum.insert(era_writes.begin(), era_writes.end());

		for (unsigned i = 0; i < nr_blocks; i++)
			if (filters[era]->test(i))
				filter_sum.insert(i);

		unsigned count = 0;
		for (unsigned i = 0; i < nr_blocks; i++) {
			if (write_sum.count(i) > 0)
				ASSERT_THAT(filter_sum.count(i), Gt(0ull));

			else if (filter_sum.count(i))
				count++;
		}

		cerr << "blocks >= era " << era << ", false positives = "
		     << static_cast<double>(count * 100) / static_cast<double>(nr_blocks)
		     << "%" << endl;
	}
}

//----------------------------------------------------------------

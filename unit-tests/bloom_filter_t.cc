#include "gmock/gmock.h"
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
	struct block_address_bloom_traits {
		typedef block_address value_type;

		static const uint64_t ones = ~0ull;
		static const uint64_t m1 = 0x9e37fffffffc0001UL;
		static const uint64_t m2 = ones - 82;

		static const unsigned bits = 18;

		static uint64_t hash1(block_address const &b) {
			return (b * m1) >> bits;
		}

		static uint64_t hash2(block_address const &b) {
			uint32_t n = b;

			n = n ^ (n >> 16);
			n = n * 0x85ebca6bu;
			n = n ^ (n >> 13);
			n = n * 0xc2b2ae35u;
			n = n ^ (n >> 16);

			return n;
		}

		static uint64_t hash3(block_address const &b) {
			return (b * m2) >> bits;
		}
	};

	template <class Traits>
	class bloom_filter {
	public:
		bloom_filter(unsigned nr_bits_power, unsigned nr_probes)
			: bits_(1ull << nr_bits_power, false),
			  nr_probes_(nr_probes),
			  mask_((1ull << nr_bits_power) - 1) {

			cerr << "nr entries = " << bits_.size() << ", mask = " << mask_ << endl;
		}

		bool test(typename Traits::value_type const &v) {
			vector<uint32_t> probes(nr_probes_);
			fill_probes(v, probes);

			for (unsigned p = 0; p < nr_probes_; p++)
				if (!bits_.at(probes[p]))
					return false;

			return true;
		}

		void add(typename Traits::value_type const &v) {
			vector<uint32_t> probes(nr_probes_);
			fill_probes(v, probes);

			for (unsigned p = 0; p < nr_probes_; p++) {
				//cerr << probes[p] << ", ";
				bits_.at(probes[p]) = true;
			}
			//cerr << endl;
		}

		void dump() const {
			residency();

			map<unsigned, unsigned> runs;

			for (unsigned i = 0; i < bits_.size();) {
				bool v = bits_[i];
				unsigned run_length = 1;

				while (bits_[++i] == v && i < bits_.size())
					run_length++;

				map<unsigned, unsigned>::iterator it = runs.find(run_length);
				if (it != runs.end())
					it->second++;
				else
					runs.insert(make_pair(run_length, 1));
			}

			{
				map<unsigned, unsigned>::const_iterator it;
				for (it = runs.begin(); it != runs.end(); ++it)
					cout << it->first << ": " << it->second << endl;
			}
		}

		void residency() const {
			unsigned count = 0;
			for (unsigned i = 0; i < bits_.size(); i++)
				if (bits_[i])
					count++;

			cout << "residency: " << count << "/" << bits_.size() << endl;
		}

	private:
		void fill_probes(typename Traits::value_type const &v, vector<uint32_t> &probes) {
			uint32_t h1 = Traits::hash1(v) & mask_;
			uint32_t h2 = Traits::hash2(v) & mask_;

			probes[0] = h1;
			for (unsigned p = 1; p < nr_probes_; p++) {
				h1 = (h1 + h2) & mask_;
				h2 = (h2 + p) & mask_;
				probes[p] = h1;
			}
		}

		vector<bool> bits_;
		unsigned nr_probes_;
		uint64_t mask_;
	};

	//--------------------------------
#if 0
	class dm_era {
	public:
		dm_era(block_address nr_blocks)
		: nr_blocks_(nr_blocks),
		  era_base_(0),
		  base_(nr_blocks, false) {
		}

		set<block_address> blocks_written_since(unsigned era) const {

		}

		unsigned get_era() const {
			return era_base_ + eras_.size() - 1;
		}

		void record_write(block_address b) {
			current_era.record_write(b);
		}

		void resize(block_address new_size) {
			nr_blocks_ = new_size;
			push_era();
			base_.resize(new_size, false);
		}

	private:
		era_details &current_era() {
			return eras_.back();
		}

		void need_new_era() {
			// ???
		}

		void push_era() {
			eras_.push_back(era(nr_blocks_));
			if (eras_.size() > 100)
				pop_era();
		}

		void pop_era() {
			era_base_++;



			eras_.pop_front();
		}

		static const unsigned NR_PROBES = 6;

		class era_details {
		public:
			era_details(block_address nr_blocks)
			: nr_blocks_(nr_blocks),
			  f(power_bits(nr_blocks, NR_PROBES)) {
			}

			void record_write(block_address b) {
				f.add(b);
			}

			void add_blocks_written(set<block_address &result) const {
				for (block_address b = 0; b < nr_blocks; b++)
					if (f.test(b))
						result.insert(b);
			}

		private:
			static unsigned power_bits(block_address nr_blocks) {
				// We're expecting 1% of the cache to change per era
				block_address expected_writes = nr_blocks / 100;

				unsigned r = 1;
				while ((1ull << r) < (16 * expected_writes))
					r++;

				return r;
			}

			typedef bloom_filter<block_address_bloom_traits> filter;

			block_address nr_blocks;
			filter f;
		};

		block_address nr_blocks_;
		unsigned era_base_;
		vector<bool> base_;
		deque<era_details> eras_;
	};
#endif

	//--------------------------------

	class BloomFilterTests : public Test {
	public:

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
	};
}

//----------------------------------------------------------------

TEST_F(BloomFilterTests, can_create_a_bloom_filter)
{
	bloom_filter<block_address_bloom_traits> f(10, 3);
}

TEST_F(BloomFilterTests, no_false_negatives)
{
	bloom_filter<block_address_bloom_traits> f(12, 6);
	set<block_address> bs = generate_random_blocks(1000);

	set<block_address>::const_iterator it;
	for (it = bs.begin(); it != bs.end(); ++it)
		f.add(*it);

	for (it = bs.begin(); it != bs.end(); ++it)
		ASSERT_THAT(f.test(*it), Eq(true));

	f.dump();
}

TEST_F(BloomFilterTests, count_false_positives)
{
	block_address nr_blocks = 128 * 1024 * 1024;
	block_address written_blocks = nr_blocks / 100;

	unsigned shift = 1;

	while ((1ull << shift) < (16 * written_blocks))
		shift++;
	cerr << "bitset " << ((1 << shift) / (8 * 1024)) << "k" << endl;

	bloom_filter<block_address_bloom_traits> f(shift, 6);
	set<block_address> bs = generate_random_blocks(written_blocks, nr_blocks);
	set<block_address>::const_iterator it;

	for (it = bs.begin(); it != bs.end(); ++it)
		f.add(*it);

	f.dump();

	unsigned count = 0;
	for (unsigned i = 0; i < nr_blocks; i++)
		if (!bs.count(i) && f.test(i))
			count++;

	cerr << count << "false positives out of " << nr_blocks << endl;
	cerr << static_cast<double>(count * 100) / static_cast<double>(nr_blocks) << "%" << endl;
}

//----------------------------------------------------------------

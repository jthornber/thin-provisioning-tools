#include "persistent-data/data-structures/bloom_filter.h"

#include <stdexcept>

using namespace persistent_data;
using namespace std;

//----------------------------------------------------------------

namespace {
	static const uint64_t m1 = 0x9e37fffffffc0001ULL;
	static const unsigned bits = 18;

	static uint32_t hash1(block_address const &b) {
		return (b * m1) >> bits;
	}

	static uint32_t hash2(block_address const &b) {
		uint32_t n = b;

		n = n ^ (n >> 16);
		n = n * 0x85ebca6bu;
		n = n ^ (n >> 13);
		n = n * 0xc2b2ae35u;
		n = n ^ (n >> 16);

		return n;
	}

	void check_power_of_two(unsigned nr_bits) {
		if (nr_bits & (nr_bits - 1))
			throw std::runtime_error("bloom filter needs a power of two nr_bits");
	}
}

//----------------------------------------------------------------

bloom_filter::bloom_filter(transaction_manager &tm,
			   unsigned nr_bits, unsigned nr_probes)
	: tm_(tm),
	  bits_(tm),
	  nr_probes_(nr_probes),
	  mask_(nr_bits - 1)
{
	check_power_of_two(nr_bits);
	bits_.grow(nr_bits, false);
}

bloom_filter::bloom_filter(transaction_manager &tm, block_address root,
			   unsigned nr_bits, unsigned nr_probes)
	: tm_(tm),
	  bits_(tm, root, nr_bits),
	  nr_probes_(nr_probes),
	  mask_(nr_bits - 1)
{
	check_power_of_two(nr_bits);
}

block_address
bloom_filter::get_root() const
{
	return bits_.get_root();
}

bool
bloom_filter::test(uint64_t b)
{
	vector<unsigned> probes(nr_probes_);
	fill_probes(b, probes);

	for (unsigned p = 0; p < nr_probes_; p++)
		if (!bits_.get(probes[p]))
			return false;

	return true;
}

void
bloom_filter::set(uint64_t b)
{
	vector<unsigned> probes(nr_probes_);
	fill_probes(b, probes);

	for (unsigned p = 0; p < nr_probes_; p++)
		bits_.set(probes[p], true);
}

void
bloom_filter::flush()
{
	bits_.flush();
}

void
bloom_filter::fill_probes(block_address b, std::vector<unsigned> &probes) const
{
	uint32_t h1 = hash1(b) & mask_;
	uint32_t h2 = hash2(b) & mask_;

	probes[0] = h1;
	for (unsigned p = 1; p < nr_probes_; p++) {
		h1 = (h1 + h2) & mask_;
		h2 = (h2 + p) & mask_;
		probes[p] = h1;
	}
}

void
bloom_filter::print_debug(std::ostream &out)
{
	print_residency(out);

	map<unsigned, unsigned> runs;

	for (unsigned i = 0; i < bits_.get_nr_bits();) {
		bool v = bits_.get(i);
		unsigned run_length = 1;

		while (++i < bits_.get_nr_bits() && bits_.get(i) == v)
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
			out << it->first << ": " << it->second << endl;
	}
}

void
bloom_filter::print_residency(std::ostream &out)
{
	unsigned count = 0;
	for (unsigned i = 0; i < bits_.get_nr_bits(); i++)
		if (bits_.get(i))
			count++;

	out << "residency: " << count << "/" << bits_.get_nr_bits() << endl;
}

//----------------------------------------------------------------

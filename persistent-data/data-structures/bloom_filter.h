#ifndef PERSISTENT_DATA_DATA_STRUCTURES_BLOOM_FILTER_H
#define PERSISTENT_DATA_DATA_STRUCTURES_BLOOM_FILTER_H

#include "persistent-data/transaction_manager.h"
#include "persistent-data/data-structures/bitset.h"

#include <boost/shared_ptr.hpp>

//----------------------------------------------------------------

namespace persistent_data {
	class bloom_filter {
	public:
		typedef boost::shared_ptr<bloom_filter> ptr;

		// nr_bits must be a power of two
		bloom_filter(transaction_manager &tm,
			     unsigned nr_bits, unsigned nr_probes);

		bloom_filter(transaction_manager &tm, block_address root,
			     unsigned nr_bits_power, unsigned nr_probes);

		block_address get_root() const;

		bool test(uint64_t b); // not const due to caching effects in bitset
		void set(uint64_t b);
		void flush();

		void print_debug(std::ostream &out);

	private:
		void print_residency(std::ostream &out);

		void fill_probes(block_address b, std::vector<unsigned> &probes) const;

		transaction_manager &tm_;
		persistent_data::bitset bits_;
		unsigned nr_probes_;
		uint64_t mask_;
	};
}

//----------------------------------------------------------------

#endif

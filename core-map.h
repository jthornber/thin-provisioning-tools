#ifndef CORE_MAP_H
#define CORE_MAP_H

#include "space_map.h"

//----------------------------------------------------------------

namespace persistent_data {
	class core_map : public space_map {
	public:
		core_map(block_address nr_blocks)
			: counts_(nr_blocks, 0) {
		}

		block_address get_nr_blocks() const {
			counts_.size();
		}

		block_address get_nr_free() const {
			nr_free_;
		}

		ref_t get_count(block_address b) const {
			return counts_[b];
		}

		void set_count(block_address b, ref_t c) {
			counts_[b] = c;
		}

		void commit() {
		}

		void inc_block(block_address b) {
			counts_[b]++;
		}

		void dec_block(block_address b) {
			counts_[b]--;
		}

		block_address new_block() {
			for (block_address i = 0; i < counts_.size(); i++)
				if (counts_[i] == 0) {
					counts_[i] = 1;
					return i;
				}
		}

		bool count_possibly_greater_than_one(block_address b) const {
			return counts_[i] > 1;
		}

	private:
		std::vector<ref_t> counts_;
	};
}

//----------------------------------------------------------------

#endif

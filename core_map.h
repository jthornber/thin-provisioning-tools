#ifndef CORE_MAP_H
#define CORE_MAP_H

#include "space_map.h"

//----------------------------------------------------------------

namespace persistent_data {
	class core_map : public space_map {
	public:
		core_map(block_address nr_blocks)
			: counts_(nr_blocks, 0),
			  nr_free_(nr_blocks) {
		}

		block_address get_nr_blocks() const {
			return counts_.size();
		}

		block_address get_nr_free() const {
			return nr_free_;
		}

		ref_t get_count(block_address b) const {
			return counts_[b];
		}

		void set_count(block_address b, ref_t c) {
			if (counts_[b] == 0 && c > 0)
				nr_free_--;

			else if (counts_[b] > 0 && c == 0)
				nr_free_++;

			counts_[b] = c;
		}

		void commit() {
		}

		void inc(block_address b) {
			if (counts_[b] == 0)
				nr_free_--;

			counts_[b]++;
		}

		void dec(block_address b) {
			counts_[b]--;

			if (counts_[b] == 0)
				nr_free_++;
		}

		block_address new_block() {
			for (block_address i = 0; i < counts_.size(); i++)
				if (counts_[i] == 0) {
					counts_[i] = 1;
					nr_free_--;
					return i;
				}

			throw std::runtime_error("no space");
		}

		bool count_possibly_greater_than_one(block_address b) const {
			return counts_[b] > 1;
		}

		void extend(block_address extra_blocks) {
			throw std::runtime_error("not implemented");
		}

	private:
		std::vector<ref_t> counts_;
		unsigned nr_free_;
	};
}

//----------------------------------------------------------------

#endif

// Copyright (C) 2011 Red Hat, Inc. All rights reserved.
//
// This file is part of the thin-provisioning-tools source.
//
// thin-provisioning-tools is free software: you can redistribute it
// and/or modify it under the terms of the GNU General Public License
// as published by the Free Software Foundation, either version 3 of
// the License, or (at your option) any later version.
//
// thin-provisioning-tools is distributed in the hope that it will be
// useful, but WITHOUT ANY WARRANTY; without even the implied warranty
// of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License along
// with thin-provisioning-tools.  If not, see
// <http://www.gnu.org/licenses/>.

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

		maybe_block new_block() {
			return new_block(0, counts_.size());
		}

		maybe_block new_block(block_address begin, block_address end) {
			for (block_address i = begin; i < std::min<block_address>(end, counts_.size()); i++)
				if (counts_[i] == 0) {
					counts_[i] = 1;
					nr_free_--;
					return i;
				}

			return maybe_block();
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

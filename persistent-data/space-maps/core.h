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

#include "persistent-data/space_map.h"

//----------------------------------------------------------------

namespace persistent_data {
	class core_map : public checked_space_map {
	public:
		typedef boost::shared_ptr<core_map> ptr;

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

		maybe_block new_block(span_iterator &it) {
			for (maybe_span ms = it.first(); ms; ms = it.next()) {
				for (block_address b = ms->first; b < ms->second; b++) {
					if (b >= counts_.size())
						throw std::runtime_error("block out of bounds");

					if (!counts_[b]) {
						counts_[b] = 1;
						nr_free_--;
						return maybe_block(b);
					}
				}
			}

			return maybe_block();
		}

		bool count_possibly_greater_than_one(block_address b) const {
			return counts_[b] > 1;
		}

		void extend(block_address extra_blocks) {
			throw std::runtime_error("'extend' not implemented");
		}

		// FIXME: meaningless, but this class is only used for testing
		size_t root_size() const {
			return 0;
		}

		// FIXME: meaningless, but this class is only used for testing
		virtual void copy_root(void *dest, size_t len) const {
			throw std::runtime_error("'copy root' not implemented");
		}

		checked_space_map::ptr clone() const {
			return ptr(new core_map(*this));
		}

	private:
		std::vector<ref_t> counts_;
		unsigned nr_free_;
	};
}

//----------------------------------------------------------------

#endif

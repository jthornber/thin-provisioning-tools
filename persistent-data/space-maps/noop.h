// Copyright (C) 2019 Red Hat, Inc. All rights reserved.
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

#ifndef NOOP_MAP_H
#define NOOP_MAP_H

#include "persistent-data/space_map.h"

//----------------------------------------------------------------

namespace persistent_data {
	class noop_map : public checked_space_map {
	public:
		typedef std::shared_ptr<noop_map> ptr;

		block_address get_nr_blocks() const {
			fail();
			return 0;
		}

		block_address get_nr_free() const {
			fail();
			return 0;
		}

		ref_t get_count(block_address b) const {
			fail();
			return 0;
		}

		void set_count(block_address b, ref_t c) {
			fail();
		}

		void commit() {
			fail();
		}

		void inc(block_address b, ref_t count) {
			fail();
		}

		void dec(block_address b, ref_t count) {
			fail();
		}

		maybe_block find_free(span_iterator &it) {
			fail();
			return boost::optional<block_address>(0);
		}

		bool count_possibly_greater_than_one(block_address b) const {
			fail();
			return false;
		}

		void extend(block_address extra_blocks) {
			fail();
		}

		void count_metadata(block_counter &bc) const {
			fail();
		}

		size_t root_size() const {
			fail();
			return 0;
		}

		virtual void copy_root(void *dest, size_t len) const {
			fail();
		}

		checked_space_map::ptr clone() const {
			return ptr(new noop_map());
		}

	private:
		void fail() const {
			throw std::runtime_error("noop space map used");
		}
	};
}

//----------------------------------------------------------------

#endif


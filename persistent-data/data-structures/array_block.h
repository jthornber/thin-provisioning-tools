// Copyright (C) 2012 Red Hat, Inc. All rights reserved.
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

#ifndef ARRAY_BLOCK_H
#define ARRAY_BLOCK_H

#include "persistent-data/endian_utils.h"

//----------------------------------------------------------------

namespace persistent_data {
	struct array_block_disk {
		base::__le32 csum;
		base::__le32 max_entries;
		base::__le32 nr_entries;
		base::__le32 value_size;
		base::__le64 blocknr;
	} __attribute__((packed));

	// RefType should be either a read_ref or write_ref from block_manager
	template <typename ValueTraits, typename RefType>
	class array_block {
	public:
		typedef boost::shared_ptr<array_block> ptr;
		typedef typename ValueTraits::disk_type disk_type;
		typedef typename ValueTraits::value_type value_type;
		typedef typename ValueTraits::ref_counter ref_counter;

		array_block(RefType ref,
			    ref_counter rc,
			    uint32_t value_size)
			: ref_(ref),
			  rc_(rc) {

			using namespace base;
			struct array_block_disk *header = get_header();
			header->max_entries = to_disk<__le32>(calc_max_entries(value_size));
			header->nr_entries = to_disk<__le32>(static_cast<uint32_t>(0));
			header->value_size = to_disk<__le32>(value_size);
		}

		array_block(RefType ref, ref_counter rc)
			: ref_(ref),
			  rc_(rc) {
		}

		uint32_t max_entries() const {
			return base::to_cpu<uint32_t>(get_header()->max_entries);
		}

		uint32_t nr_entries() const {
			return base::to_cpu<uint32_t>(get_header()->nr_entries);
		}

		uint32_t value_size() const {
			return base::to_cpu<uint32_t>(get_header()->value_size);
		}

		void grow(uint32_t nr, value_type const &default_value) {
			uint32_t old_nr = nr_entries();

			if (nr >= max_entries())
				throw runtime_error("array_block index out of bounds");

			if (nr <= old_nr)
				throw runtime_error("array_block grow method called with smaller size");

			grow_(nr, default_value);
		}

		void shrink(uint32_t nr) {
			uint32_t old_nr = nr_entries();

			if (nr >= old_nr)
				throw runtime_error("array_block shrink called with larger size");

			shrink_(nr);
		}

		value_type get(unsigned index) const {
			value_type v;

			ValueTraits::unpack(element_at(index), v);
			return v;
		}

		void set(unsigned index, value_type const &new_value) {
			value_type const old_value = get(index);
			rc_.inc(new_value);
			ValueTraits::pack(new_value, element_at(index));
			rc_.dec(old_value);
		}

		void inc_all_entries() {
			unsigned e = nr_entries();

			for (unsigned index = 0; index < e; index++)
				rc_.inc(get(index));
		}

		void dec_all_entries() {
			unsigned e = nr_entries();

			for (unsigned index = 0; index < e; index++)
				rc_.dec(get(index));
		}

		ref_counter const &get_ref_counter() const {
			return rc_;
		}

	private:
		static uint32_t calc_max_entries(uint32_t value_size) {
			return (RefType::BLOCK_SIZE - sizeof(array_block_disk)) / value_size;
		}

		void set_nr_entries(uint32_t nr) {
			using namespace base;
			array_block_disk *h = get_header();
			h->nr_entries = to_disk<__le32>(nr);
		}

		void grow_(uint32_t nr, value_type const &default_value) {
			uint32_t old_nr_entries = nr_entries();
			set_nr_entries(nr);

			for (unsigned i = old_nr_entries; i < nr; i++) {
				ValueTraits::pack(default_value, element_at(i));
				rc_.inc(default_value);
			}
		}

		void shrink_(uint32_t nr) {
			for (unsigned i = nr_entries() - 1; i >= nr; i--)
				rc_.dec(get(i));

			set_nr_entries(nr);
		}

		array_block_disk *get_header() {
			return reinterpret_cast<array_block_disk *>(ref_.data().raw());
		}

		array_block_disk const *get_header() const {
			return reinterpret_cast<array_block_disk const *>(ref_.data().raw());
		}

		disk_type &element_at(unsigned int index) {
			if (index >= nr_entries())
				throw runtime_error("array_block index out of bounds");

			array_block_disk *a = get_header();
			disk_type *elts = reinterpret_cast<disk_type *>(a + 1);
			return elts[index];
		}

		disk_type const &element_at(unsigned int index) const {
			if (index >= nr_entries())
				throw runtime_error("array_block index out of bounds");

			array_block_disk const *a = get_header();
			disk_type const *elts = reinterpret_cast<disk_type const *>(a + 1);
			return elts[index];
		}

		RefType ref_;
		ref_counter rc_;
	};
}

//----------------------------------------------------------------

#endif

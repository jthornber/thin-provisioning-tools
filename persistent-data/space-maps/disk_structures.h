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

#ifndef SPACE_MAP_DISK_STRUCTURES_H
#define SPACE_MAP_DISK_STRUCTURES_H

#include "base/endian_utils.h"

// FIXME: what's this included for?
#include "persistent-data/data-structures/btree.h"

//----------------------------------------------------------------

namespace persistent_data {
	using namespace base;

	namespace sm_disk_detail {
		struct index_entry_disk {
			le64 blocknr_;
			le32 nr_free_;
			le32 none_free_before_;
		} __attribute__ ((packed));

		struct index_entry {
			uint64_t blocknr_;
			uint32_t nr_free_;
			uint32_t none_free_before_;
		};

		inline bool operator==(index_entry const& lhs, index_entry const& rhs) {
			// The return value doesn't matter, since the ref-counts of bitmap blocks
			// are managed by shadow operations.
			return false;
		}

		inline bool operator!=(index_entry const& lhs, index_entry const& rhs) {
			return !(lhs == rhs);
		}

		struct index_entry_traits {
			typedef index_entry_disk disk_type;
			typedef index_entry value_type;
			typedef no_op_ref_counter<index_entry> ref_counter;

			static void unpack(disk_type const &disk, value_type &value) {
				value.blocknr_ = to_cpu<uint64_t>(disk.blocknr_);
				value.nr_free_ = to_cpu<uint32_t>(disk.nr_free_);
				value.none_free_before_ = to_cpu<uint32_t>(disk.none_free_before_);
			}

			static void pack(value_type const &value, disk_type &disk) {
				disk.blocknr_ = to_disk<le64>(value.blocknr_);
				disk.nr_free_ = to_disk<le32>(value.nr_free_);
				disk.none_free_before_ = to_disk<le32>(value.none_free_before_);
			}
		};

		unsigned const MAX_METADATA_BITMAPS = 255;
		unsigned const MAX_METADATA_BLOCKS = (255 * ((1 << 14) - 64));
		unsigned const ENTRIES_PER_BYTE = 4;

		struct metadata_index {
			le32 csum_;
			le32 padding_;
			le64 blocknr_;

			struct index_entry_disk index[MAX_METADATA_BITMAPS];
		} __attribute__ ((packed));

		struct sm_root_disk {
			le64 nr_blocks_;
			le64 nr_allocated_;
			le64 bitmap_root_;
			le64 ref_count_root_;
		} __attribute__ ((packed));

		struct sm_root {
			uint64_t nr_blocks_;
			uint64_t nr_allocated_;
			uint64_t bitmap_root_;
			uint64_t ref_count_root_;
		};

		struct sm_root_traits {
			typedef sm_root_disk disk_type;
			typedef sm_root value_type;
			typedef no_op_ref_counter<sm_root> ref_counter;

			static void unpack(disk_type const &disk, value_type &value) {
				value.nr_blocks_ = to_cpu<uint64_t>(disk.nr_blocks_);
				value.nr_allocated_ = to_cpu<uint64_t>(disk.nr_allocated_);
				value.bitmap_root_ = to_cpu<uint64_t>(disk.bitmap_root_);
				value.ref_count_root_ = to_cpu<uint64_t>(disk.ref_count_root_);
			}

			static void pack(value_type const &value, disk_type &disk) {
				disk.nr_blocks_ = to_disk<le64>(value.nr_blocks_);
				disk.nr_allocated_ = to_disk<le64>(value.nr_allocated_);
				disk.bitmap_root_ = to_disk<le64>(value.bitmap_root_);
				disk.ref_count_root_ = to_disk<le64>(value.ref_count_root_);
			}
		};

		struct bitmap_header {
			le32 csum;
			le32 not_used;
			le64 blocknr;
		} __attribute__ ((packed));

		uint64_t const BITMAP_CSUM_XOR = 240779;
		uint64_t const INDEX_CSUM_XOR = 160478;
	}
}

//----------------------------------------------------------------

#endif

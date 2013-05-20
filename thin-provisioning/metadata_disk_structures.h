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

#ifndef METADATA_DISK_STRUCTURES_H
#define METADATA_DISK_STRUCTURES_H

#include "persistent-data/endian_utils.h"
#include "persistent-data/data-structures/btree.h"

//----------------------------------------------------------------

namespace thin_provisioning {
	using namespace base;	// FIXME: don't use namespaces in headers.

	unsigned const SPACE_MAP_ROOT_SIZE = 128;

	typedef unsigned char __u8;

	struct superblock_disk {
		le32 csum_;
		le32 flags_;
		le64 blocknr_;

		__u8 uuid_[16];
		le64 magic_;
		le32 version_;
		le32 time_;

		le64 trans_id_;
		/* root for userspace's transaction (for migration and friends) */
		le64 metadata_snap_;

		__u8 data_space_map_root_[SPACE_MAP_ROOT_SIZE];
		__u8 metadata_space_map_root_[SPACE_MAP_ROOT_SIZE];

		/* 2 level btree mapping (dev_id, (dev block, time)) -> data block */
		le64 data_mapping_root_;

		/* device detail root mapping dev_id -> device_details */
		le64 device_details_root_;

		le32 data_block_size_; /* in 512-byte sectors */

		le32 metadata_block_size_; /* in 512-byte sectors */
		le64 metadata_nr_blocks_;

		le32 compat_flags_;
		le32 compat_ro_flags_;
		le32 incompat_flags_;
	} __attribute__ ((packed));

	struct superblock {
		uint32_t csum_;
		uint32_t flags_;
		uint64_t blocknr_;

		unsigned char uuid_[16];
		uint64_t magic_;
		uint32_t version_;
		uint32_t time_;

		uint64_t trans_id_;
		/* root for userspace's transaction (for migration and friends) */
		uint64_t metadata_snap_;

		unsigned char data_space_map_root_[SPACE_MAP_ROOT_SIZE];
		unsigned char metadata_space_map_root_[SPACE_MAP_ROOT_SIZE];

		/* 2 level btree mapping (dev_id, (dev block, time)) -> data block */
		uint64_t data_mapping_root_;

		/* device detail root mapping dev_id -> device_details */
		uint64_t device_details_root_;

		uint32_t data_block_size_; /* in 512-byte sectors */

		uint32_t metadata_block_size_; /* in 512-byte sectors */
		uint64_t metadata_nr_blocks_;

		uint32_t compat_flags_;
		uint32_t compat_ro_flags_;
		uint32_t incompat_flags_;
	};

	struct superblock_traits {
		typedef superblock_disk disk_type;
		typedef superblock value_type;
		typedef no_op_ref_counter<superblock> ref_counter;

		static void unpack(superblock_disk const &disk, superblock &core);
		static void pack(superblock const &core, superblock_disk &disk);
	};

	block_address const SUPERBLOCK_LOCATION = 0;
}

//----------------------------------------------------------------

#endif

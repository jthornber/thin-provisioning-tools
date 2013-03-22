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

#ifndef CACHE_METADATA_DISK_STRUCTURES_H
#define CACHE_METADATA_DISK_STRUCTURES_H

#include "persistent-data/endian_utils.h"
#include "persistent-data/data-structures/btree.h"

//----------------------------------------------------------------

// FIXME: rename to just METADATA_DISK_STRUCTURES
namespace cache_tools {
	using namespace base;	// FIXME: don't use namespaces in headers.

	unsigned const SPACE_MAP_ROOT_SIZE = 128;
	unsigned const CACHE_POLICY_NAME_SIZE = 16;
	unsigned const CACHE_POLICY_VERSION_SIZE = 3;

	typedef unsigned char __u8;

	struct superblock_disk {
		le32 csum;
		le32 flags;
		le64 blocknr;

		__u8 uuid[16];
		le64 magic;
		le32 version;

		__u8 policy_name[CACHE_POLICY_NAME_SIZE];
		le32 policy_version[CACHE_POLICY_VERSION_SIZE];
		le32 policy_hint_size;

		__u8 metadata_space_map_root[SPACE_MAP_ROOT_SIZE];

		le64 mapping_root;
		le64 hint_root;

		le64 discard_root;
		le64 discard_block_size;
		le64 discard_nr_blocks;

		le32 data_block_size; /* in 512-byte sectors */
		le32 metadata_block_size; /* in 512-byte sectors */
		le32 cache_blocks;

		le32 compat_flags;
		le32 compat_ro_flags;
		le32 incompat_flags;

		le32 read_hits;
		le32 read_misses;
		le32 write_hits;
		le32 write_misses;
	} __attribute__ ((packed));

	struct superblock {
		uint32_t csum;
		uint32_t flags;
		uint64_t blocknr;

		__u8 uuid[16];
		uint64_t magic;
		uint32_t version;

		__u8 policy_name[CACHE_POLICY_NAME_SIZE];
		uint32_t policy_version[CACHE_POLICY_VERSION_SIZE];
		uint32_t policy_hint_size;

		__u8 metadata_space_map_root[SPACE_MAP_ROOT_SIZE];

		uint64_t mapping_root;
		uint64_t hint_root;

		uint64_t discard_root;
		uint64_t discard_block_size;
		uint64_t discard_nr_blocks;

		uint32_t data_block_size; /* in 512-byte sectors */
		uint32_t metadata_block_size; /* in 512-byte sectors */
		uint32_t cache_blocks;

		uint32_t compat_flags;
		uint32_t compat_ro_flags;
		uint32_t incompat_flags;

		uint32_t read_hits;
		uint32_t read_misses;
		uint32_t write_hits;
		uint32_t write_misses;
	};

	struct superblock_traits {
		typedef superblock_disk disk_type;
		typedef superblock value_type;

		static void unpack(superblock_disk const &disk, superblock &value);
		static void pack(superblock const &value, superblock_disk &disk);
	};
}

//----------------------------------------------------------------

#endif

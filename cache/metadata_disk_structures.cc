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

#include "metadata_disk_structures.h"

#include <string.h>

using namespace cache_tools;

//----------------------------------------------------------------

void
superblock_traits::unpack(superblock_disk const &disk, superblock &core)
{
	core.csum = to_cpu<uint32_t>(disk.csum);
	core.flags = to_cpu<uint32_t>(disk.flags);
	core.blocknr = to_cpu<uint64_t>(disk.blocknr);

	::memcpy(core.uuid, disk.uuid, sizeof(core.uuid));
	core.magic = to_cpu<uint64_t>(disk.magic);
	core.version = to_cpu<uint32_t>(disk.version);

	::memcpy(core.policy_name, disk.policy_name, sizeof(core.policy_name));

	for (unsigned i = 0; i < CACHE_POLICY_VERSION_SIZE; i++)
		core.policy_version[i] = to_cpu<uint32_t>(disk.policy_version[i]);

	core.policy_hint_size = to_cpu<uint32_t>(disk.policy_hint_size);

	::memcpy(core.metadata_space_map_root,
		 disk.metadata_space_map_root,
		 sizeof(core.metadata_space_map_root));

	core.mapping_root = to_cpu<uint64_t>(disk.mapping_root);
	core.hint_root = to_cpu<uint64_t>(disk.hint_root);

	core.discard_root = to_cpu<uint64_t>(disk.discard_root);
	core.discard_block_size = to_cpu<uint64_t>(disk.discard_block_size);
	core.discard_nr_blocks = to_cpu<uint64_t>(disk.discard_nr_blocks);

	core.data_block_size = to_cpu<uint32_t>(disk.data_block_size);
	core.metadata_block_size = to_cpu<uint32_t>(disk.metadata_block_size);
	core.cache_blocks = to_cpu<uint32_t>(disk.cache_blocks);

	core.compat_flags = to_cpu<uint32_t>(disk.compat_flags);
	core.compat_ro_flags = to_cpu<uint32_t>(disk.compat_ro_flags);
	core.incompat_flags = to_cpu<uint32_t>(disk.incompat_flags);

	core.read_hits = to_cpu<uint32_t>(disk.read_hits);
	core.read_misses = to_cpu<uint32_t>(disk.read_misses);
	core.write_hits = to_cpu<uint32_t>(disk.write_hits);
	core.write_misses = to_cpu<uint32_t>(disk.write_misses);
}

void
superblock_traits::pack(superblock const &core, superblock_disk &disk)
{
	disk.csum = to_disk<le32>(core.csum);
	disk.flags = to_disk<le32>(core.flags);
	disk.blocknr = to_disk<le64>(core.blocknr);

	::memcpy(disk.uuid, core.uuid, sizeof(disk.uuid));
	disk.magic = to_disk<le64>(core.magic);
	disk.version = to_disk<le32>(core.version);

	::memcpy(disk.policy_name, core.policy_name, sizeof(disk.policy_name));

	for (unsigned i = 0; i < CACHE_POLICY_VERSION_SIZE; i++)
		disk.policy_version[i] = to_disk<le32>(core.policy_version[i]);

	disk.policy_hint_size = to_disk<le32>(core.policy_hint_size);

	::memcpy(disk.metadata_space_map_root,
		 core.metadata_space_map_root,
		 sizeof(disk.metadata_space_map_root));

	disk.mapping_root = to_disk<le64>(core.mapping_root);
	disk.hint_root = to_disk<le64>(core.hint_root);

	disk.discard_root = to_disk<le64>(core.discard_root);
	disk.discard_block_size = to_disk<le64>(core.discard_block_size);
	disk.discard_nr_blocks = to_disk<le64>(core.discard_nr_blocks);

	disk.data_block_size = to_disk<le32>(core.data_block_size);
	disk.metadata_block_size = to_disk<le32>(core.metadata_block_size);
	disk.cache_blocks = to_disk<le32>(core.cache_blocks);

	disk.compat_flags = to_disk<le32>(core.compat_flags);
	disk.compat_ro_flags = to_disk<le32>(core.compat_ro_flags);
	disk.incompat_flags = to_disk<le32>(core.incompat_flags);

	disk.read_hits = to_disk<le32>(core.read_hits);
	disk.read_misses = to_disk<le32>(core.read_misses);
	disk.write_hits = to_disk<le32>(core.write_hits);
	disk.write_misses = to_disk<le32>(core.write_misses);
}

//----------------------------------------------------------------

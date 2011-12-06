// Copyright (C) 20011 Red Hat, Inc. All rights reserved.
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

using namespace thin_provisioning;

//----------------------------------------------------------------

void
device_details_traits::unpack(device_details_disk const &disk, device_details &value)
{
	value.mapped_blocks_ = to_cpu<uint64_t>(disk.mapped_blocks_);
	value.transaction_id_ = to_cpu<uint64_t>(disk.transaction_id_);
	value.creation_time_ = to_cpu<uint32_t>(disk.creation_time_);
	value.snapshotted_time_ = to_cpu<uint32_t>(disk.snapshotted_time_);
}

void
device_details_traits::pack(device_details const &value, device_details_disk &disk)
{
	disk.mapped_blocks_ = to_disk<__le64>(value.mapped_blocks_);
	disk.transaction_id_ = to_disk<__le64>(value.transaction_id_);
	disk.creation_time_ = to_disk<__le32>(value.creation_time_);
	disk.snapshotted_time_ = to_disk<__le32>(value.snapshotted_time_);
}

void
superblock_traits::unpack(superblock_disk const &disk, superblock &value)
{
	value.csum_ = to_cpu<uint32_t>(disk.csum_);
	value.flags_ = to_cpu<uint32_t>(disk.csum_);
	value.blocknr_ = to_cpu<uint64_t>(disk.blocknr_);

	::memcpy(value.uuid_, disk.uuid_, sizeof(value.uuid_));
	value.magic_ = to_cpu<uint64_t>(disk.magic_);
	value.version_ = to_cpu<uint32_t>(disk.version_);
	value.time_ = to_cpu<uint32_t>(disk.time_);

	value.trans_id_ = to_cpu<uint64_t>(disk.trans_id_);
	value.held_root_ = to_cpu<uint64_t>(disk.held_root_);

	::memcpy(value.data_space_map_root_,
		 disk.data_space_map_root_,
		 sizeof(value.data_space_map_root_));
	::memcpy(value.metadata_space_map_root_,
		 disk.metadata_space_map_root_,
		 sizeof(value.metadata_space_map_root_));

	value.data_mapping_root_ = to_cpu<uint64_t>(disk.data_mapping_root_);
	value.device_details_root_ = to_cpu<uint64_t>(disk.device_details_root_);
	value.data_block_size_ = to_cpu<uint32_t>(disk.data_block_size_);

	value.metadata_block_size_ = to_cpu<uint32_t>(disk.metadata_block_size_);
	value.metadata_nr_blocks_ = to_cpu<uint64_t>(disk.metadata_nr_blocks_);

	value.compat_flags_ = to_cpu<uint32_t>(disk.compat_flags_);
	value.incompat_flags_ = to_cpu<uint32_t>(disk.incompat_flags_);
}

void
superblock_traits::pack(superblock const &value, superblock_disk &disk)
{
	disk.csum_ = to_disk<__le32>(value.csum_);
	disk.flags_ = to_disk<__le32>(value.csum_);
	disk.blocknr_ = to_disk<__le64>(value.blocknr_);

	::memcpy(disk.uuid_, value.uuid_, sizeof(disk.uuid_));
	disk.magic_ = to_disk<__le64>(value.magic_);
	disk.version_ = to_disk<__le32>(value.version_);
	disk.time_ = to_disk<__le32>(value.time_);

	disk.trans_id_ = to_disk<__le64>(value.trans_id_);
	disk.held_root_ = to_disk<__le64>(value.held_root_);

	::memcpy(disk.data_space_map_root_,
		 value.data_space_map_root_,
		 sizeof(disk.data_space_map_root_));
	::memcpy(disk.metadata_space_map_root_,
		 value.metadata_space_map_root_,
		 sizeof(disk.metadata_space_map_root_));

	disk.data_mapping_root_ = to_disk<__le64>(value.data_mapping_root_);
	disk.device_details_root_ = to_disk<__le64>(value.device_details_root_);
	disk.data_block_size_ = to_disk<__le32>(value.data_block_size_);

	disk.metadata_block_size_ = to_disk<__le32>(value.metadata_block_size_);
	disk.metadata_nr_blocks_ = to_disk<__le64>(value.metadata_nr_blocks_);

	disk.compat_flags_ = to_disk<__le32>(value.compat_flags_);
	disk.incompat_flags_ = to_disk<__le32>(value.incompat_flags_);
}

//----------------------------------------------------------------

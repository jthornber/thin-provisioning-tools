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

#include "thin-provisioning/device_tree.h"
#include "thin-provisioning/metadata.h"

#include "persistent-data/file_utils.h"
#include "persistent-data/math_utils.h"
#include "persistent-data/space-maps/core.h"
#include "persistent-data/space-maps/disk.h"

#include <linux/fs.h>
#include <sys/ioctl.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>

using namespace base;
using namespace thin_provisioning;

//----------------------------------------------------------------

namespace {
	using namespace superblock_detail;

	void
	copy_space_maps(space_map::ptr lhs, space_map::ptr rhs) {
		for (block_address b = 0; b < rhs->get_nr_blocks(); b++) {
			uint32_t count = rhs->get_count(b);
			if (count > 0)
				lhs->set_count(b, rhs->get_count(b));
		}
	}
}

//----------------------------------------------------------------

metadata::metadata(block_manager::ptr bm, open_type ot,
		   sector_t data_block_size,
		   block_address nr_data_blocks)
{
	switch (ot) {
	case OPEN:
		tm_ = open_tm(bm, SUPERBLOCK_LOCATION);
		sb_ = read_superblock(tm_->get_bm());

		if (sb_.version_ < METADATA_VERSION)
			throw runtime_error("unknown metadata version");

		metadata_sm_ = open_metadata_sm(*tm_, &sb_.metadata_space_map_root_);
		tm_->set_sm(metadata_sm_);

		data_sm_ = open_disk_sm(*tm_, static_cast<void *>(&sb_.data_space_map_root_));
		details_ = device_tree::ptr(new device_tree(*tm_, sb_.device_details_root_,
							    device_tree_detail::device_details_traits::ref_counter()));
		mappings_top_level_ = dev_tree::ptr(new dev_tree(*tm_, sb_.data_mapping_root_,
								 mapping_tree_detail::mtree_ref_counter(*tm_)));
		mappings_ = mapping_tree::ptr(new mapping_tree(*tm_, sb_.data_mapping_root_,
							       mapping_tree_detail::block_time_ref_counter(data_sm_)));
		break;

	case CREATE:
		tm_ = open_tm(bm, SUPERBLOCK_LOCATION);
		space_map::ptr core = tm_->get_sm();
		metadata_sm_ = create_metadata_sm(*tm_, tm_->get_bm()->get_nr_blocks());
		copy_space_maps(metadata_sm_, core);
		tm_->set_sm(metadata_sm_);

		data_sm_ = create_disk_sm(*tm_, nr_data_blocks);
		details_ = device_tree::ptr(new device_tree(*tm_,
							    device_tree_detail::device_details_traits::ref_counter()));
		mappings_ = mapping_tree::ptr(new mapping_tree(*tm_,
							       mapping_tree_detail::block_time_ref_counter(data_sm_)));
		mappings_top_level_ = dev_tree::ptr(new dev_tree(*tm_, mappings_->get_root(),
								 mapping_tree_detail::mtree_ref_counter(*tm_)));

		::memset(&sb_, 0, sizeof(sb_));
		sb_.magic_ = SUPERBLOCK_MAGIC;
		sb_.version_ = METADATA_VERSION;
		sb_.data_mapping_root_ = mappings_->get_root();
		sb_.device_details_root_ = details_->get_root();
		sb_.data_block_size_ = data_block_size;
		sb_.metadata_block_size_ = MD_BLOCK_SIZE >> SECTOR_SHIFT;
		sb_.metadata_nr_blocks_ = metadata_sm_->get_nr_blocks();

		break;
	}
}

metadata::metadata(block_manager::ptr bm, bool read_space_maps)
{
	tm_ = open_tm(bm, SUPERBLOCK_LOCATION);
	sb_ = read_superblock(tm_->get_bm(), SUPERBLOCK_LOCATION);

	if (read_space_maps)
		open_space_maps();

	open_btrees();
}

metadata::metadata(block_manager::ptr bm,
		   boost::optional<block_address> metadata_snap)
{
	tm_ = open_tm(bm, SUPERBLOCK_LOCATION);

	superblock_detail::superblock actual_sb = read_superblock(bm, SUPERBLOCK_LOCATION);

	if (!actual_sb.metadata_snap_)
		throw runtime_error("no current metadata snap");

	if (metadata_snap && *metadata_snap != actual_sb.metadata_snap_)
			throw runtime_error("metadata snapshot does not match that in superblock");

	sb_ = read_superblock(bm, actual_sb.metadata_snap_);

	// metadata snaps don't record the space maps
	open_btrees();
}

metadata::metadata(block_manager::ptr bm, superblock_detail::superblock const &sb)
{
	tm_ = open_tm(bm, SUPERBLOCK_LOCATION);
	sb_ = sb;
	open_btrees();
}

void
metadata::commit()
{
	sb_.data_mapping_root_ = mappings_->get_root();
	sb_.device_details_root_ = details_->get_root();

	data_sm_->commit();
	data_sm_->copy_root(&sb_.data_space_map_root_, sizeof(sb_.data_space_map_root_));

	metadata_sm_->commit();
	metadata_sm_->copy_root(&sb_.metadata_space_map_root_, sizeof(sb_.metadata_space_map_root_));

	write_ref superblock = tm_->get_bm()->superblock_zero(SUPERBLOCK_LOCATION, superblock_validator());
        superblock_disk *disk = reinterpret_cast<superblock_disk *>(superblock.data());
	superblock_traits::pack(sb_, *disk);
}

void metadata::open_space_maps()
{
	metadata_sm_ = open_metadata_sm(*tm_, &sb_.metadata_space_map_root_);
	tm_->set_sm(metadata_sm_);

	data_sm_ = open_disk_sm(*tm_, static_cast<void *>(&sb_.data_space_map_root_));
}

void metadata::open_btrees()
{
	details_ = device_tree::ptr(new device_tree(*tm_, sb_.device_details_root_,
						    device_tree_detail::device_details_traits::ref_counter()));
	mappings_top_level_ = dev_tree::ptr(new dev_tree(*tm_, sb_.data_mapping_root_,
							 mapping_tree_detail::mtree_ref_counter(*tm_)));
	mappings_ = mapping_tree::ptr(new mapping_tree(*tm_, sb_.data_mapping_root_,
						       mapping_tree_detail::block_time_ref_counter(data_sm_)));
}

//----------------------------------------------------------------

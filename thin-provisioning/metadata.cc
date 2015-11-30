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

	unsigned const METADATA_CACHE_SIZE = 1024;

	transaction_manager::ptr
	open_tm(block_manager<>::ptr bm) {
		space_map::ptr sm(new core_map(bm->get_nr_blocks()));
		sm->inc(SUPERBLOCK_LOCATION);
		transaction_manager::ptr tm(new transaction_manager(bm, sm));
		return tm;
	}

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

metadata::metadata(std::string const &dev_path, open_type ot,
		   sector_t data_block_size, block_address nr_data_blocks)
{
	switch (ot) {
	case OPEN:
		tm_ = open_tm(open_bm(dev_path, block_manager<>::READ_ONLY));
		sb_ = read_superblock(tm_->get_bm());

		if (sb_.version_ != 1)
			throw runtime_error("unknown metadata version");

		metadata_sm_ = open_metadata_sm(*tm_, &sb_.metadata_space_map_root_);
		tm_->set_sm(metadata_sm_);

		data_sm_ = open_disk_sm(*tm_, static_cast<void *>(&sb_.data_space_map_root_));

		details_ = device_tree::ptr(
			new device_tree(*tm_, sb_.device_details_root_,
					device_tree_detail::device_details_traits::ref_counter()));

		mappings_top_level_ = dev_tree::ptr(
			new dev_tree(*tm_, sb_.data_mapping_root_,
				     mapping_tree_detail::mtree_ref_counter(tm_)));

		mappings_ = mapping_tree::ptr(
			new mapping_tree(*tm_, sb_.data_mapping_root_,
					 mapping_tree_detail::block_time_ref_counter(data_sm_)));
		break;

	case CREATE:
		tm_ = open_tm(open_bm(dev_path, block_manager<>::READ_WRITE));
		space_map::ptr core = tm_->get_sm();
		metadata_sm_ = create_metadata_sm(*tm_, tm_->get_bm()->get_nr_blocks());
		copy_space_maps(metadata_sm_, core);
		tm_->set_sm(metadata_sm_);

		data_sm_ = create_disk_sm(*tm_, nr_data_blocks);
		details_ = device_tree::ptr(new device_tree(*tm_, device_tree_detail::device_details_traits::ref_counter()));
		mappings_ = mapping_tree::ptr(new mapping_tree(*tm_,
							       mapping_tree_detail::block_time_ref_counter(data_sm_)));
		mappings_top_level_ = dev_tree::ptr(new dev_tree(*tm_, mappings_->get_root(),
								 mapping_tree_detail::mtree_ref_counter(tm_)));

		::memset(&sb_, 0, sizeof(sb_));
		sb_.magic_ = SUPERBLOCK_MAGIC;
		sb_.version_ = 1;
		sb_.data_mapping_root_ = mappings_->get_root();
		sb_.device_details_root_ = details_->get_root();
		sb_.data_block_size_ = data_block_size;
		sb_.metadata_block_size_ = MD_BLOCK_SIZE >> SECTOR_SHIFT;
		sb_.metadata_nr_blocks_ = tm_->get_bm()->get_nr_blocks();

		break;
	}
}

metadata::metadata(std::string const &dev_path, block_address metadata_snap)
{
	tm_ = open_tm(open_bm(dev_path, block_manager<>::READ_ONLY, !metadata_snap));
	sb_ = read_superblock(tm_->get_bm(), metadata_snap);

	// We don't open the metadata sm for a held root
	//metadata_sm_ = open_metadata_sm(tm_, &sb_.metadata_space_map_root_);
	//tm_->set_sm(metadata_sm_);

	data_sm_ = open_disk_sm(*tm_, static_cast<void *>(&sb_.data_space_map_root_));
	details_ = device_tree::ptr(new device_tree(*tm_, sb_.device_details_root_, device_tree_detail::device_details_traits::ref_counter()));
	mappings_top_level_ = dev_tree::ptr(new dev_tree(*tm_, sb_.data_mapping_root_,
							 mapping_tree_detail::mtree_ref_counter(tm_)));
	mappings_ = mapping_tree::ptr(new mapping_tree(*tm_, sb_.data_mapping_root_,
						       mapping_tree_detail::block_time_ref_counter(data_sm_)));
}

// FIXME: duplication
metadata::metadata(block_manager<>::ptr bm, open_type ot,
		   sector_t data_block_size,
		   block_address nr_data_blocks)
{
	switch (ot) {
	case OPEN:
		tm_ = open_tm(bm);
		sb_ = read_superblock(tm_->get_bm());

		if (sb_.version_ != 1)
			throw runtime_error("unknown metadata version");

		metadata_sm_ = open_metadata_sm(*tm_, &sb_.metadata_space_map_root_);
		tm_->set_sm(metadata_sm_);

		data_sm_ = open_disk_sm(*tm_, static_cast<void *>(&sb_.data_space_map_root_));
		details_ = device_tree::ptr(new device_tree(*tm_, sb_.device_details_root_, device_tree_detail::device_details_traits::ref_counter()));
		mappings_top_level_ = dev_tree::ptr(new dev_tree(*tm_, sb_.data_mapping_root_,
								 mapping_tree_detail::mtree_ref_counter(tm_)));
		mappings_ = mapping_tree::ptr(new mapping_tree(*tm_, sb_.data_mapping_root_,
							       mapping_tree_detail::block_time_ref_counter(data_sm_)));
		break;

	case CREATE:
		tm_ = open_tm(bm);
		space_map::ptr core = tm_->get_sm();
		metadata_sm_ = create_metadata_sm(*tm_, tm_->get_bm()->get_nr_blocks());
		copy_space_maps(metadata_sm_, core);
		tm_->set_sm(metadata_sm_);

		data_sm_ = create_disk_sm(*tm_, nr_data_blocks);
		details_ = device_tree::ptr(new device_tree(*tm_, device_tree_detail::device_details_traits::ref_counter()));
		mappings_ = mapping_tree::ptr(new mapping_tree(*tm_,
							       mapping_tree_detail::block_time_ref_counter(data_sm_)));
		mappings_top_level_ = dev_tree::ptr(new dev_tree(*tm_, mappings_->get_root(),
								 mapping_tree_detail::mtree_ref_counter(tm_)));

		::memset(&sb_, 0, sizeof(sb_));
		sb_.magic_ = SUPERBLOCK_MAGIC;
		sb_.version_ = 1;
		sb_.data_mapping_root_ = mappings_->get_root();
		sb_.device_details_root_ = details_->get_root();
		sb_.data_block_size_ = data_block_size;
		sb_.metadata_block_size_ = MD_BLOCK_SIZE >> SECTOR_SHIFT;
		sb_.metadata_nr_blocks_ = tm_->get_bm()->get_nr_blocks();

		break;
	}
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

//----------------------------------------------------------------

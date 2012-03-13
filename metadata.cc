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

#include "metadata.h"

#include "math_utils.h"
#include "space_map_core.h"
#include "space_map_disk.h"

#include <linux/fs.h>
#include <sys/ioctl.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>

using namespace thin_provisioning;

//----------------------------------------------------------------

namespace {
	uint32_t const SUPERBLOCK_MAGIC = 27022010;
        uint32_t const VERSION = 1;
	unsigned const METADATA_CACHE_SIZE = 1024;
        unsigned const SECTOR_TO_BLOCK_SHIFT = 3;
	uint32_t const SUPERBLOCK_CSUM_SEED = 160774;

	struct superblock_validator : public block_manager<>::validator {
		virtual void check(block_manager<>::const_buffer &b, block_address location) const {
			superblock_disk const *sbd = reinterpret_cast<superblock_disk const *>(&b);
			crc32c sum(SUPERBLOCK_CSUM_SEED);
			sum.append(&sbd->flags_, MD_BLOCK_SIZE - sizeof(uint32_t));
			if (sum.get_sum() != to_cpu<uint32_t>(sbd->csum_))
				throw checksum_error("bad checksum in superblock");
		}

		virtual void prepare(block_manager<>::buffer &b, block_address location) const {
			superblock_disk *sbd = reinterpret_cast<superblock_disk *>(&b);
			crc32c sum(SUPERBLOCK_CSUM_SEED);
			sum.append(&sbd->flags_, MD_BLOCK_SIZE - sizeof(uint32_t));
			sbd->csum_ = to_disk<base::__le32>(sum.get_sum());
		}
	};

	block_address
	get_nr_blocks(string const &path) {
		struct stat info;
		block_address nr_blocks;

		int r = ::stat(path.c_str(), &info);
		if (r)
			throw runtime_error("Couldn't stat dev path");

		if (S_ISREG(info.st_mode))
			nr_blocks = div_up<block_address>(info.st_size, MD_BLOCK_SIZE);

		else if (S_ISBLK(info.st_mode)) {
			// To get the size of a block device we need to
			// open it, and then make an ioctl call.
			int fd = ::open(path.c_str(), O_RDONLY);
			if (fd < 0)
				throw runtime_error("couldn't open block device to ascertain size");

			r = ::ioctl(fd, BLKGETSIZE64, &nr_blocks);
			if (r) {
				::close(fd);
				throw runtime_error("ioctl BLKGETSIZE64 failed");
			}
			::close(fd);
			nr_blocks = div_down<block_address>(nr_blocks, MD_BLOCK_SIZE);
		} else
			throw runtime_error("bad path");

		return nr_blocks;
	}

	transaction_manager::ptr
	open_tm(string const &dev_path, bool writeable) {
		block_address nr_blocks = get_nr_blocks(dev_path);
		block_manager<>::ptr bm(new block_manager<>(dev_path, nr_blocks, 1, writeable));
		space_map::ptr sm(new core_map(nr_blocks));
		sm->inc(SUPERBLOCK_LOCATION);
		transaction_manager::ptr tm(new transaction_manager(bm, sm));
		return tm;
	}

	superblock
	read_superblock(block_manager<>::ptr bm) {
		superblock sb;
		block_manager<>::read_ref r = bm->read_lock(SUPERBLOCK_LOCATION,
							    mk_validator(new superblock_validator));
		superblock_disk const *sbd = reinterpret_cast<superblock_disk const *>(&r.data());
		superblock_traits::unpack(*sbd, sb);
		return sb;
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
		tm_ = open_tm(dev_path, false);
		sb_ = read_superblock(tm_->get_bm());
		metadata_sm_ = open_metadata_sm(tm_, &sb_.metadata_space_map_root_);
		tm_->set_sm(metadata_sm_);

		data_sm_ = open_disk_sm(tm_, static_cast<void *>(&sb_.data_space_map_root_));
		details_ = detail_tree::ptr(new detail_tree(tm_, sb_.device_details_root_, device_details_traits::ref_counter()));
		mappings_top_level_ = dev_tree::ptr(new dev_tree(tm_, sb_.data_mapping_root_, mtree_ref_counter(tm_)));
		mappings_ = mapping_tree::ptr(new mapping_tree(tm_, sb_.data_mapping_root_, block_time_ref_counter(data_sm_)));
		break;

	case CREATE:
		tm_ = open_tm(dev_path, true);
		space_map::ptr core = tm_->get_sm();
		metadata_sm_ = create_metadata_sm(tm_, tm_->get_bm()->get_nr_blocks());
		copy_space_maps(metadata_sm_, core);
		tm_->set_sm(metadata_sm_);

		data_sm_ = create_disk_sm(tm_, nr_data_blocks);
		details_ = detail_tree::ptr(new detail_tree(tm_, device_details_traits::ref_counter()));
		mappings_ = mapping_tree::ptr(new mapping_tree(tm_, block_time_ref_counter(data_sm_)));
		mappings_top_level_ = dev_tree::ptr(new dev_tree(tm_, mappings_->get_root(), mtree_ref_counter(tm_)));

		::memset(&sb_, 0, sizeof(sb_));
		sb_.magic_ = SUPERBLOCK_MAGIC;
		sb_.version_ = 1;
		sb_.data_mapping_root_ = mappings_->get_root();
		sb_.device_details_root_ = details_->get_root();
		sb_.data_block_size_ = data_block_size;
		sb_.metadata_block_size_ = MD_BLOCK_SIZE;
		sb_.metadata_nr_blocks_ = tm_->get_bm()->get_nr_blocks();

		break;
	}
}

namespace {
	void print_superblock(superblock const &sb) {
		using namespace std;

		cerr << "superblock " << sb.csum_ << endl
		     << "flags " << sb.flags_ << endl
		     << "blocknr " << sb.blocknr_ << endl
		     << "transaction id " << sb.trans_id_ << endl
		     << "data mapping root " << sb.data_mapping_root_ << endl
		     << "details root " << sb.device_details_root_ << endl
		     << "data block size " << sb.data_block_size_ << endl
		     << "metadata block size " << sb.metadata_block_size_ << endl
		     << "metadata nr blocks " << sb.metadata_nr_blocks_ << endl
			;
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
	print_superblock(sb_);

	superblock_validator v;
	write_ref superblock = tm_->get_bm()->superblock_zero(SUPERBLOCK_LOCATION,
							      mk_validator(new superblock_validator()));
        superblock_disk *disk = reinterpret_cast<superblock_disk *>(superblock.data());
	superblock_traits::pack(sb_, *disk);
}

//----------------------------------------------------------------

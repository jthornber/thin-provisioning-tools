#include "metadata.h"

#include "math_utils.h"
#include "space_map_disk.h"
#include "space_map_core.h"

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

	block_address get_nr_blocks(string const &path) {
		struct stat info;
		block_address nr_blocks;

		int r = ::stat(path.c_str(), &info);
		if (r)
			throw runtime_error("Couldn't stat dev path");

		if (S_ISREG(info.st_mode))
			nr_blocks = div_down<block_address>(info.st_size, MD_BLOCK_SIZE);

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
	open_tm(string const &dev_path) {
		block_address nr_blocks = get_nr_blocks(dev_path);
		block_manager<>::ptr bm(new block_manager<>(dev_path, nr_blocks, 8));
		space_map::ptr sm(new core_map(nr_blocks));
		transaction_manager::ptr tm(new transaction_manager(bm, sm));
		return tm;
	}

	superblock read_superblock(block_manager<>::ptr bm) {
		superblock sb;
		block_manager<>::read_ref r = bm->read_lock(SUPERBLOCK_LOCATION);
		superblock_disk const *sbd = reinterpret_cast<superblock_disk const *>(&r.data());

		crc32c sum(160774); // FIXME: magic number
		sum.append(&sbd->flags_, MD_BLOCK_SIZE - sizeof(uint32_t));
		if (sum.get_sum() != to_cpu<uint32_t>(sbd->csum_)) {
			ostringstream out;
			out << "bad checksum in superblock, calculated "
			    << sum.get_sum()
			    << ", superblock contains " << to_cpu<uint32_t>(sbd->csum_);
			throw runtime_error(out.str());
		}

		superblock_traits::unpack(*sbd, sb);
		return sb;
	}
}

//----------------------------------------------------------------

metadata::metadata(std::string const &dev_path, open_type ot)
	: tm_(open_tm(dev_path)),
	  sb_(read_superblock(tm_->get_bm())),
	  metadata_sm_(open_metadata_sm(tm_, static_cast<void *>(&sb_.metadata_space_map_root_))),
	  data_sm_(open_disk_sm(tm_, static_cast<void *>(&sb_.data_space_map_root_))),
	  details_(new detail_tree(tm_, sb_.device_details_root_, device_details_traits::ref_counter())),
	  mappings_top_level_(new dev_tree(tm_, sb_.data_mapping_root_, mtree_ref_counter(tm_))),
	  mappings_(new mapping_tree(tm_, sb_.data_mapping_root_, block_time_ref_counter(data_sm_)))
{
	tm_->set_sm(open_metadata_sm(tm_, sb_.metadata_space_map_root_));
}

#if 0
	::memset(&sb_, 0, sizeof(sb_));
	sb_.data_mapping_root_ = mappings_.get_root();
	sb_.device_details_root_ = details_.get_root();
	sb_.metadata_block_size_ = MD_BLOCK_SIZE;
	sb_.metadata_nr_blocks_ = tm_->get_bm()->get_nr_blocks();
#endif

void
metadata::commit()
{
	sb_.data_mapping_root_ = mappings_->get_root();
	sb_.device_details_root_ = details_->get_root();

	// FIXME: commit and copy the space map roots

	write_ref superblock = tm_->get_bm()->superblock(SUPERBLOCK_LOCATION);
        superblock_disk *disk = reinterpret_cast<superblock_disk *>(superblock.data());
	superblock_traits::pack(sb_, *disk);
}

//----------------------------------------------------------------

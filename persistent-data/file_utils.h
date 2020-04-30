#ifndef THIN_FILE_UTILS_H
#define THIN_FILE_UTILS_H

#include "persistent-data/block.h"
#include "persistent-data/transaction_manager.h"

#include <string>

//----------------------------------------------------------------

// FIXME: move to a different unit
namespace persistent_data {
	bool check_for_xml(block_manager::ptr bm);
	persistent_data::block_address get_nr_blocks(std::string const &path, sector_t block_size = MD_BLOCK_SIZE);
	block_address get_nr_metadata_blocks(std::string const &path);

	block_manager::ptr open_bm(std::string const &dev_path,
				   block_manager::mode m, bool excl = true);

	block_manager::ptr open_bm(std::string const &path);
	transaction_manager::ptr open_tm(block_manager::ptr bm,
					 block_address superblock_location);

}

//----------------------------------------------------------------

#endif

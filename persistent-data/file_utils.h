#ifndef THIN_FILE_UTILS_H
#define THIN_FILE_UTILS_H

#include "persistent-data/block.h"

#include <string>

//----------------------------------------------------------------

// FIXME: move to a different unit
namespace persistent_data {
	persistent_data::block_address get_nr_blocks(std::string const &path, sector_t block_size = MD_BLOCK_SIZE);
	block_address get_nr_metadata_blocks(std::string const &path);

	block_manager<>::ptr open_bm(std::string const &dev_path,
				     block_manager<>::mode m, bool excl = true);
}

//----------------------------------------------------------------

#endif

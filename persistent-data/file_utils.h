#ifndef THIN_FILE_UTILS_H
#define THIN_FILE_UTILS_H

#include "persistent-data/block.h"

//----------------------------------------------------------------

namespace persistent_data {
	persistent_data::block_address get_nr_blocks(string const &path);
	block_manager<>::ptr open_bm(std::string const &dev_path, block_io<>::mode m);
}

//----------------------------------------------------------------

#endif

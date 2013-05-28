#ifndef THIN_FILE_UTILS_H
#define THIN_FILE_UTILS_H

#include "persistent-data/block.h"

//----------------------------------------------------------------

namespace thin_provisioning {
	persistent_data::block_address get_nr_blocks(string const &path);
}

//----------------------------------------------------------------

#endif

#ifndef PERSISTENT_DATA_VALIDATORS_H
#define PERSISTENT_DATA_VALIDATORS_H

#include "block-cache/block_cache.h"

//----------------------------------------------------------------

namespace persistent_data {
	bcache::validator::ptr create_btree_node_validator();
}

//----------------------------------------------------------------

#endif

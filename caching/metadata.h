#ifndef CACHE_METADATA_H
#define CACHE_METADATA_H

#include "persistent-data/block.h"
#include "persistent-data/data-structures/array.h"
#include "persistent-data/endian_utils.h"
#include "persistent-data/space-maps/disk.h"
#include "persistent-data/transaction_manager.h"

#include "caching/superblock.h"

//----------------------------------------------------------------

namespace caching {
	class metadata {
	public:
		enum open_type {
			CREATE,
			OPEN
		};

		typedef block_manager<>::read_ref read_ref;
		typedef block_manager<>::write_ref write_ref;
		typedef boost::shared_ptr<metadata> ptr;

		metadata(block_manager<>::ptr bm, open_type ot);
		metadata(block_manager<>::ptr bm, block_address metadata_snap);
	};
};

//----------------------------------------------------------------

#endif

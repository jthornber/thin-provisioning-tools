#ifndef METADATA_COUNTER_H
#define METADATA_COUNTER_H

#include "thin-provisioning/metadata.h"
#include "persistent-data/data-structures/btree_counter.h"

//----------------------------------------------------------------

namespace thin_provisioning {
	bool count_metadata(transaction_manager::ptr tm,
			    superblock_detail::superblock const &sb,
			    block_counter &bc,
			    bool skip_metadata_snap = false,
			    bool ignore_non_fatal = false);
}

//----------------------------------------------------------------

#endif

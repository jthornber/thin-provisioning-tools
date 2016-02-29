#ifndef METADATA_COUNTER_H
#define METADATA_COUNTER_H

#include "thin-provisioning/metadata.h"
#include "persistent-data/data-structures/btree_counter.h"

//----------------------------------------------------------------

namespace thin_provisioning {
	void count_trees(transaction_manager::ptr tm,
			 superblock_detail::superblock &sb,
			 block_counter &bc);
	void count_space_maps(transaction_manager::ptr tm,
			      superblock_detail::superblock &sb,
			      block_counter &bc);
	void count_metadata(transaction_manager::ptr tm,
			    superblock_detail::superblock &sb,
			    block_counter &bc,
			    bool skip_metadata_snap = false);
}

//----------------------------------------------------------------

#endif

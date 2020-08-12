#include "thin-provisioning/metadata_counter.h"
#include "persistent-data/space-maps/core.h"
#include "persistent-data/space-maps/disk_structures.h"

using namespace persistent_data;
using namespace thin_provisioning;

//----------------------------------------------------------------

namespace {
	bool count_trees(transaction_manager::ptr tm,
			 superblock_detail::superblock const &sb,
			 block_counter &bc) {

		// Count the device tree
		{
			noop_value_counter<device_tree_detail::device_details> vc;
			device_tree dtree(*tm, sb.device_details_root_,
					  device_tree_detail::device_details_traits::ref_counter());
			count_btree_blocks(dtree, bc, vc);
		}

		// Count the mapping tree
		{
			noop_value_counter<mapping_tree_detail::block_time> vc;
			mapping_tree mtree(*tm, sb.data_mapping_root_,
					   mapping_tree_detail::block_traits::ref_counter(space_map::ptr()));
			count_btree_blocks(mtree, bc, vc);
		}

		return true;
	}

	bool count_space_maps(transaction_manager::ptr tm,
			      superblock_detail::superblock const &sb,
			      block_counter &bc) {
		bool ret = true;

		// Count the metadata space map (no-throw)
		try {
			persistent_space_map::ptr metadata_sm =
				open_metadata_sm(*tm, static_cast<void const *>(&sb.metadata_space_map_root_));
			metadata_sm->count_metadata(bc);
		} catch (std::exception &e) {
			cerr << e.what() << endl;
			ret = false;
		}

		// Count the data space map (no-throw)
		try {
			persistent_space_map::ptr data_sm =
				open_disk_sm(*tm, static_cast<void const *>(&sb.data_space_map_root_));
			data_sm->count_metadata(bc);
		} catch (std::exception &e) {
			cerr << e.what() << endl;
			ret = false;
		}

		return ret;
	}
}

//----------------------------------------------------------------

bool thin_provisioning::count_metadata(transaction_manager::ptr tm,
				       superblock_detail::superblock const &sb,
				       block_counter &bc,
				       bool skip_metadata_snap) {
	bool ret = true;

	// Count the superblock
	bc.inc(superblock_detail::SUPERBLOCK_LOCATION);
	ret &= count_trees(tm, sb, bc);

	// Count the metadata snap, if present
	if (!skip_metadata_snap && sb.metadata_snap_ != superblock_detail::SUPERBLOCK_LOCATION) {
		bc.inc(sb.metadata_snap_);

		superblock_detail::superblock snap = read_superblock(tm->get_bm(), sb.metadata_snap_);
		ret &= count_trees(tm, snap, bc);
	}

	ret &= count_space_maps(tm, sb, bc);

	return ret;
}

//----------------------------------------------------------------

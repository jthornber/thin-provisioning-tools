#ifndef SPACE_MAP_DISK_H
#define SPACE_MAP_DISK_H

#include "btree_checker.h"
#include "space_map.h"

//----------------------------------------------------------------

namespace persistent_data {
	class checked_space_map : public persistent_space_map {
	public:
		typedef boost::shared_ptr<checked_space_map> ptr;

		virtual void check(block_counter &counter) const = 0;
	};

	checked_space_map::ptr
	create_disk_sm(transaction_manager::ptr tm, block_address nr_blocks);

	checked_space_map::ptr
	open_disk_sm(transaction_manager::ptr tm, void *root);

	checked_space_map::ptr
	create_metadata_sm(transaction_manager::ptr tm, block_address nr_blocks);

	checked_space_map::ptr
	open_metadata_sm(transaction_manager::ptr tm, void *root);
}

//----------------------------------------------------------------

#endif

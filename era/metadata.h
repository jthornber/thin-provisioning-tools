#ifndef ERA_METADATA_H
#define ERA_METADATA_H

#include "base/endian_utils.h"

#include "persistent-data/block.h"
#include "persistent-data/data-structures/array.h"
#include "persistent-data/data-structures/bitset.h"
#include "persistent-data/space-maps/disk.h"
#include "persistent-data/transaction_manager.h"

#include "era/superblock.h"
#include "era/writeset_tree.h"
#include "era/era_array.h"

//----------------------------------------------------------------

namespace era {
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
		void commit();

		typedef persistent_data::transaction_manager tm;
		tm::ptr tm_;
		superblock sb_;
		checked_space_map::ptr metadata_sm_;
		writeset_tree::ptr writeset_tree_;
		era_array::ptr era_array_;

	private:
		void create_metadata(block_manager<>::ptr bm);
		void open_metadata(block_manager<>::ptr bm,
				   block_address loc = SUPERBLOCK_LOCATION);

		void commit_space_map();
		void commit_writesets();
		void commit_era_array();
		void commit_superblock();
	};
};

//----------------------------------------------------------------

#endif

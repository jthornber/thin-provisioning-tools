// Copyright (C) 2011 Red Hat, Inc. All rights reserved.
//
// This file is part of the thin-provisioning-tools source.
//
// thin-provisioning-tools is free software: you can redistribute it
// and/or modify it under the terms of the GNU General Public License
// as published by the Free Software Foundation, either version 3 of
// the License, or (at your option) any later version.
//
// thin-provisioning-tools is distributed in the hope that it will be
// useful, but WITHOUT ANY WARRANTY; without even the implied warranty
// of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License along
// with thin-provisioning-tools.  If not, see
// <http://www.gnu.org/licenses/>.

#ifndef METADATA_LL_H
#define METADATA_LL_H

#include "persistent-data/block.h"
#include "persistent-data/data-structures/btree.h"
#include "persistent-data/endian_utils.h"
#include "persistent-data/space-maps/disk.h"
#include "persistent-data/transaction_manager.h"

#include "thin-provisioning/device_tree.h"
#include "thin-provisioning/mapping_tree.h"
#include "thin-provisioning/superblock.h"

//----------------------------------------------------------------

namespace thin_provisioning {
	// FIXME: don't use namespaces in a header
	using namespace base;
	using namespace persistent_data;

	typedef uint64_t sector_t;
	typedef uint32_t thin_dev_t;

	//------------------------------------------------

	// FIXME: should these be in a sub-namespace?
	typedef persistent_data::transaction_manager::ptr tm_ptr;

	// The tools require different interfaces onto the metadata than
	// the in kernel driver.  This class gives access to the low-level
	// implementation of metadata.  Implement more specific interfaces
	// on top of this.
	struct metadata {
		enum open_type {
			CREATE,
			OPEN
		};

		typedef block_manager<>::read_ref read_ref;
		typedef block_manager<>::write_ref write_ref;
		typedef boost::shared_ptr<metadata> ptr;


		// Deprecated: it would be better if we passed in an already
		// constructed block_manager.
		metadata(std::string const &dev_path, open_type ot,
			 sector_t data_block_size = 128,
			 block_address nr_data_blocks = 0); // Only used if CREATE

		metadata(std::string const &dev_path, block_address metadata_snap);

		// ... use these instead ...
		metadata(block_manager<>::ptr bm, open_type ot,
			 sector_t data_block_size = 128,
			 block_address nr_data_blocks = 0); // Only used if CREATE
		metadata(block_manager<>::ptr bm, block_address metadata_snap);

		void commit();


		tm_ptr tm_;
		superblock_detail::superblock sb_;

		checked_space_map::ptr metadata_sm_;
		checked_space_map::ptr data_sm_;
		device_tree::ptr details_;
		dev_tree::ptr mappings_top_level_;
		mapping_tree::ptr mappings_;
	};
}

//----------------------------------------------------------------

#endif

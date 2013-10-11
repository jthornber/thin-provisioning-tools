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

#ifndef CACHE_METADATA_H
#define CACHE_METADATA_H

#include "block.h"
#include "array.h"
#include "endian_utils.h"
#include "space_map_disk.h"
#include "transaction_manager.h"

//----------------------------------------------------------------

namespace cache {
	// FIXME: don't use namespaces in a header
	using namespace base;
	using namespace persistent_data;

	block_address const SUPERBLOCK_LOCATION = 0;

	typedef uint64_t sector_t;

	//------------------------------------------------

	class space_map_ref_counter {
	public:
		space_map_ref_counter(space_map::ptr sm)
			: sm_(sm) {
		}

		void inc(block_address b) {
			sm_->inc(b);
		}

		void dec(block_address b) {
			sm_->dec(b);
		}

	private:
		space_map::ptr sm_;
	};

	struct block_flags {
		uint64_t block_;
		uint32_t flags_;
	};

	class block_flags_ref_counter {
	public:
		block_flags_ref_counter(space_map::ptr sm)
			: sm_(sm) {
		}

		void inc(block_flags bf) {
			sm_->inc(bf.block_);
		}

		void dec(block_flags bf) {
			sm_->dec(bf.block_);
		}

	private:
		space_map::ptr sm_;
	};

	struct block_traits {
		typedef base::__le64 disk_type;
		typedef block_flags value_type;
		typedef block_flags_ref_counter ref_counter;

		static void unpack(disk_type const &disk, value_type &value) {
			uint64_t v = to_cpu<uint64_t>(disk);
			value.block_ = v >> 24;
			value.flags_ = v & ((1 << 24) - 1);
		}

		static void pack(value_type const &value, disk_type &disk) {
			uint64_t v = (value.block_ << 24) | value.flags_;
			disk = base::to_disk<base::__le64>(v);
		}
	};

	//------------------------------------------------

	// FIXME: should these be in a sub-namespace?
	typedef persistent_data::transaction_manager::ptr tm_ptr;
	typedef persistent_data::btree<1, device_details_traits> detail_tree;
	typedef persistent_data::btree<1, mtree_traits> dev_tree;
	typedef persistent_data::btree<2, block_traits> mapping_tree;
	typedef persistent_data::btree<1, block_traits> single_mapping_tree;

	// The tools require different interfaces onto the metadata than
	// the in kernel driver.  This class gives access to the low-level
	// implementation of metadata.  Implement more specific interfaces
	// on top of this.
	struct metadata {
		enum open_type {
			CREATE,
			OPEN
		};

		metadata(std::string const &dev_path, open_type ot,
			 sector_t data_block_size = 128,
			 block_address nr_cache_blocks = 0); // Only used if CREATE

		metadata(std::string const &dev_path, block_address metadata_snap);

		void commit();

		typedef block_manager<>::read_ref read_ref;
		typedef block_manager<>::write_ref write_ref;
		typedef boost::shared_ptr<metadata> ptr;

		tm_ptr tm_;
		superblock sb_;

		checked_space_map::ptr metadata_sm_;
		checked_space_map::ptr data_sm_;
		detail_tree::ptr details_;
		dev_tree::ptr mappings_top_level_;
		mapping_tree::ptr mappings_;
	};
}

//----------------------------------------------------------------

#endif

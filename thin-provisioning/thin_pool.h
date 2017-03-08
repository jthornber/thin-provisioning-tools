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

#ifndef MULTISNAP_METADATA_H
#define MULTISNAP_METADATA_H

#include "metadata.h"

#include <string>
#include <boost/shared_ptr.hpp>

//----------------------------------------------------------------

namespace thin_provisioning {
	// This interface is very like that in the kernel.  It'll allow us
	// to write simulators to try out different space maps etc.  Not
	// currently used by the tools.

	class thin_pool;
	class thin {
	public:
		typedef boost::shared_ptr<thin> ptr;
		typedef boost::optional<mapping_tree_detail::block_time> maybe_address;

		thin_dev_t get_dev_t() const;
		maybe_address lookup(block_address thin_block);
		bool insert(block_address thin_block, block_address data_block);
		void remove(block_address thin_block);

		void set_snapshot_time(uint32_t time);

		block_address get_mapped_blocks() const;
		void set_mapped_blocks(block_address count);

	private:
		friend class thin_pool;
		thin(thin_dev_t dev, thin_pool *pool); // FIXME: pass a reference rather than a ptr

		thin_dev_t dev_;
		thin_pool *pool_;
	};

	class thin_pool {
	public:
		typedef boost::shared_ptr<thin_pool> ptr;

		thin_pool(metadata::ptr md);
		~thin_pool();

		void create_thin(thin_dev_t dev);
		void create_snap(thin_dev_t dev, thin_dev_t origin);
		void del(thin_dev_t);

		void set_transaction_id(uint64_t id);
		uint64_t get_transaction_id() const;

		block_address get_metadata_snap() const;

		block_address alloc_data_block();
		void free_data_block(block_address b);

		// accessors
		block_address get_nr_free_data_blocks() const;
		sector_t get_data_block_size() const;
		block_address get_data_dev_size() const;

		thin::ptr open_thin(thin_dev_t);

	private:
		friend class thin;
		bool device_exists(thin_dev_t dev) const;

		metadata::ptr md_;
	};
};

//----------------------------------------------------------------

#endif

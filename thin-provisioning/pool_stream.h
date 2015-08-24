// Copyright (C) 2015 Red Hat, Inc. All rights reserved.
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

#ifndef POOL_STREAM_H
#define POOL_STREAM_H

#include "thin-provisioning/cache_stream.h"
#include "thin-provisioning/rmap_visitor.h"
#include "thin-provisioning/superblock.h"

//----------------------------------------------------------------

namespace thin_provisioning {
	class pool_stream : public chunk_stream {
	public:
		pool_stream(cache_stream &stream,
			    transaction_manager::ptr tm, superblock_detail::superblock const &sb,
			    block_address nr_blocks);

		block_address nr_chunks() const;
		void rewind();
		bool advance(block_address count = 1ull);
		block_address index() const;
		chunk const &get() const;

	private:
		typedef rmap_visitor::region region;
		typedef rmap_visitor::rmap_region rmap_region;

		// FIXME: too big to return by value
		vector<rmap_region> read_rmap(transaction_manager::ptr tm, superblock_detail::superblock const &sb,
					      block_address nr_blocks);
		void init_rmap(transaction_manager::ptr tm, superblock_detail::superblock const &sb,
			       block_address nr_blocks);
		bool advance_one();

		cache_stream &stream_;
		vector<uint32_t> block_to_thin_;
		block_address nr_mapped_;
	};
}

//----------------------------------------------------------------

#endif

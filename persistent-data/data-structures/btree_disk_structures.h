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

#ifndef PERSISTENT_DATA_BTREE_DISK_STRUCTURES_H
#define PERSISTENT_DATA_BTREE_DISK_STRUCTURES_H

#include "base/endian_utils.h"

//----------------------------------------------------------------

namespace persistent_data {
	namespace btree_detail {
		using namespace base;

		uint32_t const BTREE_CSUM_XOR = 121107;

		//------------------------------------------------
		// On disk data layout for btree nodes
		enum node_flags {
			INTERNAL_NODE = 1,
			LEAF_NODE = 1 << 1
		};

		struct node_header {
			le32 csum;
			le32 flags;
			le64 blocknr; /* which block this node is supposed to live in */

			le32 nr_entries;
			le32 max_entries;
			le32 value_size;
			le32 padding;
		} __attribute__((packed));

		struct disk_node {
			struct node_header header;
			le64 keys[0];
		} __attribute__((packed));

		enum node_type {
			INTERNAL,
			LEAF
		};
	}
}

//----------------------------------------------------------------

#endif

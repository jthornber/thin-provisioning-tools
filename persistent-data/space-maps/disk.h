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

#ifndef SPACE_MAP_DISK_H
#define SPACE_MAP_DISK_H

// FIXME: why is btree_checker needed?
#include "persistent-data/data-structures/btree_checker.h"
#include "persistent-data/space_map.h"

//----------------------------------------------------------------

namespace persistent_data {
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

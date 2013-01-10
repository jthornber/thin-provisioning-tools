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

#ifndef SPACE_MAP_TRANSACTIONAL_H
#define SPACE_MAP_TRANSACTIONAL_H

#include "space_map.h"

//----------------------------------------------------------------

namespace persistent_data {

	// FIXME: change name 'transactional' is so vague.

	// This space map ensures no blocks are allocated which have been
	// freed within the current transaction.
	checked_space_map::ptr create_transactional_sm(checked_space_map::ptr sm);
}

//----------------------------------------------------------------

#endif

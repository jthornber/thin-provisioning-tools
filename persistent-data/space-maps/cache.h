// Copyright (C) 2017 Red Hat, Inc. All rights reserved.
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

#ifndef SPACE_MAP_CACHE_H
#define SPACE_MAP_CACHE_H

#include "persistent-data/space_map.h"

//----------------------------------------------------------------

namespace persistent_data {
	// This space map keeps counts in core, to avoid reading btrees all the
	// time.  It wraps a real space map however.  Cache_size must be a
	// power of 2.
	persistent_data::checked_space_map::ptr
	create_cache_sm(persistent_data::checked_space_map::ptr wrappee,
			unsigned cache_size);
}

//----------------------------------------------------------------

#endif

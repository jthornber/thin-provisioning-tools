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

#ifndef METADATA_DUMPER_H
#define METADATA_DUMPER_H

#include "emitter.h"
#include "metadata.h"

//----------------------------------------------------------------

namespace thin_provisioning {
	// Set the @repair flag if your metadata is corrupt, and you'd like
	// the dumper to do it's best to recover info.  If not set, any
	// corruption encountered will cause an exception to be thrown.
	void metadata_dump(metadata::ptr md, emitter::ptr e, bool repair);
	void metadata_dump_subtree(metadata::ptr md, emitter::ptr e, bool repair, uint64_t subtree_root);
}

//----------------------------------------------------------------

#endif

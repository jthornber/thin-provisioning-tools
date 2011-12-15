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

#include "metadata.h"
#include "core_map.h"

#define BOOST_TEST_MODULE MetadataTests
#include <boost/test/included/unit_test.hpp>

using namespace std;
using namespace boost;
using namespace persistent_data;
using namespace thin_provisioning;

//----------------------------------------------------------------

namespace {
	block_address const NR_BLOCKS = 1024;
	block_address const SUPERBLOCK = 0;

	metadata::ptr
	create_metadata() {
		auto tm = create_tm();
		return metadata::ptr(
			new metadata(tm, 0, 128, 1024000, true));
	}
}

//----------------------------------------------------------------

BOOST_AUTO_TEST_CASE(create_metadata_object)
{
	auto m = create_metadata();
}

//----------------------------------------------------------------

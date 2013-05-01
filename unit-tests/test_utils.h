// Copyright (C) 2013 Red Hat, Inc. All rights reserved.
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

#include "persistent-data/block.h"
#include "persistent-data/transaction_manager.h"

//----------------------------------------------------------------

namespace test {
	unsigned const MAX_HELD_LOCKS = 16;

	template <uint32_t BlockSize>
	typename block_manager<BlockSize>::ptr
	create_bm(block_address nr = 1024) {
		string const path("./test.data");
		int r = system("rm -f ./test.data");
		if (r < 0)
			throw runtime_error("couldn't rm -f ./test.data");

		return typename block_manager<BlockSize>::ptr(
			new block_manager<BlockSize>(path, nr, MAX_HELD_LOCKS,
						     block_io<BlockSize>::CREATE));
	}

	// Don't use this to update the metadata.
	transaction_manager::ptr open_temporary_tm(block_manager<>::ptr bm);

	void zero_block(block_manager<>::ptr bm, block_address b);
}

//----------------------------------------------------------------

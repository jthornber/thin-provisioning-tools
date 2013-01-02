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

#ifndef CHECKSUM_H
#define CHECKSUM_H

#include <stdint.h>

//----------------------------------------------------------------

namespace base {
	class crc32c {
	public:
		crc32c(uint32_t xor_value);

		void append(void const *buffer, unsigned len);
		uint32_t get_sum() const;

	private:
		uint32_t xor_value_;
		uint32_t sum_;
	};
}

//----------------------------------------------------------------

#endif

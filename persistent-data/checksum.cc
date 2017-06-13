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

#include "checksum.h"

#include <boost/crc.hpp>

using namespace base;

//----------------------------------------------------------------

crc32c::crc32c(uint32_t xor_value)
	: xor_value_(xor_value),
	  sum_(0)
{
}

void
crc32c::append(void const *buffer, unsigned len)
{
	uint32_t const powers = 0x1EDC6F41;

	boost::crc_optimal<32, powers, 0xffffffff, 0, true, true> crc;
	crc.process_bytes(buffer, len);
	sum_ = crc.checksum();
}

uint32_t
crc32c::get_sum() const
{
	return sum_ ^ xor_value_;
}

//----------------------------------------------------------------

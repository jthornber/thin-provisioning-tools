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

#include "endian_utils.h"

using namespace base;

//----------------------------------------------------------------

bool
base::test_bit_le(void const *bits, unsigned b)
{
	le64 const *w = reinterpret_cast<le64 const *>(bits);
        w += b / 64;

        uint64_t v = to_cpu<uint64_t>(*w);
        uint64_t mask = 1;
        mask = mask << (b % 64);
        return (v & mask) ? true : false;
}

void
base::set_bit_le(void *bits, unsigned b)
{
	le64 *w = reinterpret_cast<le64 *>(bits);
	w += b / 64;

	uint64_t v = to_cpu<uint64_t>(*w);
	uint64_t mask = 1;
	mask = mask << (b % 64);
	v |= mask;
	*w = to_disk<le64>(v);
}

void
base::clear_bit_le(void *bits, unsigned b)
{
	le64 *w = reinterpret_cast<le64 *>(bits);
	w += b / 64;

	uint64_t v = to_cpu<uint64_t>(*w);
	uint64_t mask = 1;
	mask = mask << (b % 64);
	mask = ~mask;
	v &= mask;
	*w = to_disk<le64>(v);
}

//----------------------------------------------------------------

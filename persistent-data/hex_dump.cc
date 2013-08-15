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

#include "hex_dump.h"

#include <iostream>
#include <iomanip>

using namespace std;

//----------------------------------------------------------------

void base::hex_dump(ostream &out, void const *data_, size_t len)
{
	unsigned char const *data = reinterpret_cast<unsigned char const *>(data_),
		*end = data + len;

	ios_base::fmtflags old_flags = out.flags();
	out << hex;

	while (data < end) {
		for (unsigned i = 0; i < 16 && data < end; i++, data++)
			out << setw(2) << setfill('0') << (unsigned) *data << " ";
		out << endl;
	}

	out.setf(old_flags);
}

//----------------------------------------------------------------

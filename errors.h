// Copyright (C) 2012 Red Hat, Inc. All rights reserved.
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

#ifndef THINP_EXCEPTION_H
#define THINP_EXCEPTION_H

#include <stdexcept>

//----------------------------------------------------------------

namespace base {
	class checksum_error : public std::runtime_error {
	public:
		explicit checksum_error(std::string const &what)
			: std::runtime_error(what) {
		}
	};
}

//----------------------------------------------------------------

#endif

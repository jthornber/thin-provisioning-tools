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

#ifndef THINP_MATH_H
#define THINP_MATH_H

//----------------------------------------------------------------

namespace base {
	// Only works for integral types
	template <typename T>
	T div_up(T const &v, T const &divisor) {
		return (v + (divisor - 1)) / divisor;
	}

	// Seemingly pointless function, but it coerces the arguments
	// nicely.
	template <typename T>
	T div_down(T const &v, T const &divisor) {
		return v / divisor;
	}
}

//----------------------------------------------------------------

#endif

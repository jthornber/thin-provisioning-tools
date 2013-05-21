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

#ifndef REF_COUNTER_H
#define REF_COUNTER_H

#include <boost/shared_ptr.hpp>

//----------------------------------------------------------------

namespace persistent_data {
	template <typename ValueType>
	class ref_counter {
	public:
		boost::shared_ptr<ref_counter<ValueType> > ptr;

		virtual ~ref_counter() {}
		virtual void set(ValueType const &v, uint32_t rc) {}
		virtual void inc(ValueType const &v) {}
		virtual void dec(ValueType const &v) {}
	};

	template <typename ValueType>
	class no_op_ref_counter : public ref_counter<ValueType> {
	};
}

//----------------------------------------------------------------

#endif

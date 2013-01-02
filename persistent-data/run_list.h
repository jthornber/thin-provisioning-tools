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

#ifndef RUN_LIST_H
#define RUN_LIST_H

#include <set>

//----------------------------------------------------------------

namespace base {
	template <typename T>
	struct run {
		run(T const &b, T const &e)
			: b_(b),
			  e_(e) {
		}

		bool operator< (run const &rhs) const {
			return b_ < rhs.b_;
		}

		T b_, e_;
	};

	template <typename T>
	class run_list {
	public:
		run_list()
			: invert_(false) {
		}

		void add_run(T const &b, T const &e);
		void sub_run(T const &b, T const &e);
		bool in_run(T const &key) const;

		void invert();
		void add(run_list<T> const &rl);
		void sub(run_list<T> const &rl);

		typedef std::set<run<T> >::const_iterator const_iterator;
		const_iterator begin() const;
		const_iterator end() const;

	private:
		bool in_run_(T const &key) const;

		bool invert_;
		std::set<run<T> > runs_;
	};
}

//----------------------------------------------------------------

#include "run_list.tcc"

#endif

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

#ifndef LOCK_TRACKER_H
#define LOCK_TRACKER_H

#include <boost/noncopyable.hpp>
#include <map>
#include <stdint.h>


//----------------------------------------------------------------

namespace persistent_data {
	class lock_tracker : private boost::noncopyable {
	public:
		lock_tracker(uint64_t low, uint64_t high);

		void read_lock(uint64_t key);
		void write_lock(uint64_t key);
		void unlock(uint64_t key);

	private:
		typedef std::map<uint64_t, int> LockMap;

		bool found(LockMap::const_iterator it) const;

		bool valid_key(uint64_t key) const;
		void check_key(uint64_t key) const;

		// Positive for read lock, negative for write lock
		LockMap locks_;

		uint64_t low_;
		uint64_t high_;
	};
}

//----------------------------------------------------------------

#endif

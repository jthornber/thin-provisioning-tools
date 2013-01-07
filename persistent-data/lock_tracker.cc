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

#include "lock_tracker.h"

#include <stdexcept>

using namespace persistent_data;
using namespace std;

//----------------------------------------------------------------

lock_tracker::lock_tracker(uint64_t low, uint64_t high)
	: low_(low),
	  high_(high)
{
}

void
lock_tracker::read_lock(uint64_t key)
{
	check_key(key);

	LockMap::const_iterator it = locks_.find(key);
	if (found(it)) {
		if (it->second < 0)
			throw runtime_error("already write locked");

		locks_.insert(make_pair(key, it->second + 1));

	} else
		locks_.insert(make_pair(key, 1));
}

void
lock_tracker::write_lock(uint64_t key)
{
	check_key(key);

	LockMap::const_iterator it = locks_.find(key);
	if (found(it))
		throw runtime_error("already locked");

	locks_.insert(make_pair(key, -1));
}

void
lock_tracker::superblock_lock(uint64_t key)
{
	if (superblock_)
		throw runtime_error("superblock already held");

	superblock_ = boost::optional<uint64_t>(key);
	try {
		write_lock(key);

	} catch (...) {
		superblock_ = boost::optional<uint64_t>();
	}
}

void
lock_tracker::unlock(uint64_t key)
{
	check_key(key);

	LockMap::const_iterator it = locks_.find(key);
	if (!found(it))
		throw runtime_error("not locked");

	if (it->second > 1)
		locks_.insert(make_pair(key, it->second - 1));
	else
		locks_.erase(key);

	if (superblock_ && *superblock_ == key)
		superblock_ = boost::optional<uint64_t>();
}

bool
lock_tracker::found(LockMap::const_iterator it) const
{
	return it != locks_.end();
}

bool
lock_tracker::valid_key(uint64_t key) const
{
	return (key >= low_ && key <= high_);
}

void
lock_tracker::check_key(uint64_t key) const
{
	if (!valid_key(key))
		throw runtime_error("invalid key");
}

//----------------------------------------------------------------


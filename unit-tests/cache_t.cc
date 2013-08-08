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

#include "gmock/gmock.h"
#include "persistent-data/cache.h"

#include <boost/shared_ptr.hpp>

using namespace boost;
using namespace base;
using namespace std;
using namespace testing;

//----------------------------------------------------------------

namespace {
	struct Thing {
		Thing()
			: key_(0),
			  desc_("default constructed") {
		}

		explicit Thing(unsigned n)
			: key_(n),
			  desc_("foo bar") {
		}

		unsigned key_;
		string desc_;
	};

	struct ThingTraits {
		typedef unsigned key_type;
		typedef Thing value_type;

		static key_type get_key(value_type const &t) {
			return t.key_;
		}
	};

	struct SharedThingTraits {
		typedef unsigned key_type;
		typedef boost::shared_ptr<Thing> value_type;

		static key_type get_key(value_type const &p) {
			return p->key_;
		}
	};
}

//----------------------------------------------------------------

TEST(CacheTests, cache_creation)
{
	cache<ThingTraits> c(16);
}

TEST(CacheTests, cache_caches)
{
	unsigned const COUNT = 16;
	cache<ThingTraits> c(COUNT);

	for (unsigned i = 0; i < COUNT; i++) {
		c.insert(Thing(i));
		c.put(Thing(i));
	}

	for (unsigned i = 0; i < COUNT; i++)
		ASSERT_TRUE(c.get(i));
}

TEST(CacheTests, new_entries_have_a_ref_count_of_one)
{
	cache<ThingTraits> c(16);

	c.insert(Thing(0));
	c.put(Thing(0));

	ASSERT_THROW(c.put(Thing(0)), runtime_error);
}

TEST(CacheTests, cache_drops_elements)
{
	unsigned const COUNT = 1024;
	unsigned const CACHE_SIZE = 16;
	cache<ThingTraits> c(CACHE_SIZE);

	for (unsigned i = 0; i < COUNT; i++) {
		c.insert(Thing(i));
		c.put(Thing(i));
	}

	for (unsigned i = 0; i < COUNT - CACHE_SIZE; i++)
		ASSERT_FALSE(c.get(i));

	for (unsigned i = COUNT - CACHE_SIZE; i < COUNT; i++)
		ASSERT_TRUE(c.get(i));
}

TEST(CacheTests, held_entries_count_towards_the_cache_limit)
{
	unsigned const CACHE_SIZE = 16;
	cache<ThingTraits> c(CACHE_SIZE);

	unsigned i;
	for (i = 0; i < CACHE_SIZE; i++)
		c.insert(Thing(i));

	ASSERT_THROW(c.insert(Thing(i)), runtime_error);
}

TEST(CacheTests, put_works)
{
	unsigned const CACHE_SIZE = 16;
	cache<ThingTraits> c(CACHE_SIZE);

	unsigned i;
	for (i = 0; i < CACHE_SIZE; i++) {
		c.insert(Thing(i));
		c.put(Thing(i));
	}

	// should succeed
	c.insert(Thing(i));
}

TEST(CacheTests, multiple_gets_works)
{
	unsigned const CACHE_SIZE = 16;
	cache<ThingTraits> c(CACHE_SIZE);

	unsigned i;
	for (i = 0; i < CACHE_SIZE; i++) {
		c.insert(Thing(i));
		c.get(i);
		c.get(i);
		c.put(Thing(i));
	}

	ASSERT_THROW(c.insert(Thing(i)), runtime_error);
}

TEST(CacheTests, shared_ptr_cache_works)
{
	unsigned const CACHE_SIZE = 16;
	cache<SharedThingTraits> c(CACHE_SIZE);

	for (unsigned i = 0; i < CACHE_SIZE; i++) {
		c.insert(shared_ptr<Thing>(new Thing(i)));
		optional<shared_ptr<Thing> > maybe_ptr = c.get(i);

		ASSERT_TRUE(maybe_ptr);
		ASSERT_THAT((*maybe_ptr)->key_, Eq(i));
	}
}

//----------------------------------------------------------------

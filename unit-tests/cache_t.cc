#include "cache.h"

#define BOOST_TEST_MODULE CacheTests
#include <boost/test/included/unit_test.hpp>

#include <boost/shared_ptr.hpp>

using namespace boost;
using namespace base;
using namespace std;

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
		typedef shared_ptr<Thing> value_type;

		static key_type get_key(value_type const &p) {
			return p->key_;
		}
	};
}

//----------------------------------------------------------------

BOOST_AUTO_TEST_CASE(cache_creation)
{
	cache<ThingTraits> c(16);
}

BOOST_AUTO_TEST_CASE(cache_caches)
{
	unsigned const COUNT = 16;
	cache<ThingTraits> c(COUNT);

	for (unsigned i = 0; i < COUNT; i++) {
		c.insert(Thing(i));
		c.put(Thing(i));
	}

	for (unsigned i = 0; i < COUNT; i++)
		BOOST_ASSERT(c.get(i));
}

BOOST_AUTO_TEST_CASE(new_entries_have_a_ref_count_of_one)
{
	cache<ThingTraits> c(16);

	c.insert(Thing(0));
	c.put(Thing(0));

	BOOST_CHECK_THROW(c.put(Thing(0)), runtime_error);
}

BOOST_AUTO_TEST_CASE(cache_drops_elements)
{
	unsigned const COUNT = 1024;
	unsigned const CACHE_SIZE = 16;
	cache<ThingTraits> c(CACHE_SIZE);

	for (unsigned i = 0; i < COUNT; i++) {
		c.insert(Thing(i));
		c.put(Thing(i));
	}

	for (unsigned i = 0; i < COUNT - CACHE_SIZE; i++)
		BOOST_ASSERT(!c.get(i));

	for (unsigned i = COUNT - CACHE_SIZE; i < COUNT; i++)
		BOOST_ASSERT(c.get(i));
}

BOOST_AUTO_TEST_CASE(held_entries_count_towards_the_cache_limit)
{
	unsigned const CACHE_SIZE = 16;
	cache<ThingTraits> c(CACHE_SIZE);

	unsigned i;
	for (i = 0; i < CACHE_SIZE; i++)
		c.insert(Thing(i));

	BOOST_CHECK_THROW(c.insert(Thing(i)), runtime_error);
}

BOOST_AUTO_TEST_CASE(put_works)
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

BOOST_AUTO_TEST_CASE(multiple_gets_works)
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

	BOOST_CHECK_THROW(c.insert(Thing(i)), runtime_error);
}

BOOST_AUTO_TEST_CASE(shared_ptr_cache_works)
{
	unsigned const CACHE_SIZE = 16;
	cache<SharedThingTraits> c(CACHE_SIZE);

	for (unsigned i = 0; i < CACHE_SIZE; i++) {
		c.insert(shared_ptr<Thing>(new Thing(i)));
		optional<shared_ptr<Thing> > maybe_ptr = c.get(i);

		BOOST_ASSERT(maybe_ptr);
		BOOST_ASSERT((*maybe_ptr)->key_ == i);
	}
}

//----------------------------------------------------------------

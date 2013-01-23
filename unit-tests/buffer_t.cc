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

// Set to one to add compile time checks.
#define	COMPILE_TIME_ERROR	0

#include "persistent-data/buffer.h"

#define BOOST_TEST_MODULE BufferTests
#include <boost/test/included/unit_test.hpp>

using namespace persistent_data;

// - Allocate several on the heap, check they have the requested
//   alignment.  Try for various Alignments.  If memalign has
//   restrictions could you document (eg, power of 2).
// - you can use the [] to set a value in a non-const instance
// - you can't use the [] to set a value in a const instance - not
//   sure how to do this, since it'll be a compile time error.
// - you can use [] to read back a value that you've set.
// - [] to read works in a const instance.
// - you can use raw() to get and set values.
// - an exception is thrown if you put too large an index in []
// - check you can't copy a buffer via a copy constructor or ==
// - (again a compile time error, just experiment so you understand
// - boost::noncopyable).

//----------------------------------------------------------------

namespace {
	template <uint32_t Size, uint32_t Alignment>
	typename buffer<Size, Alignment>::ptr
	create_buffer(void) {
		return typename buffer<Size, Alignment>::ptr(new buffer<Size, Alignment>());
	}
}

//----------------------------------------------------------------

#if COMPILE_TIME_ERROR
BOOST_AUTO_TEST_CASE(buffer_copy_fails)
{
	uint32_t const sz = 8, align = 8;
	buffer<sz, align>::ptr b1 = create_buffer<sz, align>();
	buffer<sz, align>::ptr b2;

	*b2 = *b1; // Compile time failure
}
#endif

BOOST_AUTO_TEST_CASE(buffer_8_a_8_raw_access)
{
	uint32_t const sz = 8, align = 8;
	buffer<sz, align>::ptr b = create_buffer<sz, align>();
	unsigned char *p = b->raw();
	// unsigned char const *pc = b->raw();

	p[0] = 0;
	BOOST_CHECK(p[0] == 0);
	p[0] = 4;
	BOOST_CHECK(p[0] == 4);
}

#if COMPILE_TIME_ERROR
BOOST_AUTO_TEST_CASE(buffer_8_a_8_raw_const_access)
{
	uint32_t const sz = 8, align = 8;
	buffer<sz, align>::ptr b = create_buffer<sz, align>();
	unsigned char const *pc = b->raw();

	pc[0] = 5; // Compile time error accessing read-only location
}
#endif

BOOST_AUTO_TEST_CASE(buffer_8_a_8_access)
{
	uint32_t const sz = 8, align = 8;
	buffer<sz, align>::ptr b = create_buffer<sz, align>();

	(*b)[0] = 0;
	BOOST_CHECK_EQUAL((*b)[0], 0);
}

#if COMPILE_TIME_ERROR
BOOST_AUTO_TEST_CASE(buffer_8_a_8_const_access)
{
	uint32_t const sz = 8, align = 8;
	buffer<sz, align>::const_ptr b = create_buffer<sz, align>();

	(*b)[0] = 0; // Compile time error accessing read-only location 
}
#endif

BOOST_AUTO_TEST_CASE(buffer_8_a_8_oob)
{
	uint32_t const sz = 8, align = 8;
	buffer<sz, align>::ptr b = create_buffer<sz, align>();

	BOOST_CHECK_THROW((*b)[8], std::range_error);
}

// 8 byte buffer size, varying alignment from 1 - 7
#if COMPILE_TIME_ERROR
BOOST_AUTO_TEST_CASE(buffer_128_a_1_fails)
{
	uint32_t const sz = 128, align = 1;
	buffer<sz, align>::ptr b = create_buffer<sz, align>();

	BOOST_CHECK(!b);
}
#endif

BOOST_AUTO_TEST_CASE(buffer_128_a_2_succeeds)
{
	uint32_t const sz = 128, align = 2;
	buffer<sz, align>::ptr b = create_buffer<sz, align>();

	BOOST_CHECK(b);
}

#if COMPILE_TIME_ERROR
BOOST_AUTO_TEST_CASE(buffer_128_a_3_fails)
{
	uint32_t const sz = 128, align = 3;
	buffer<sz, align>::ptr b = create_buffer<sz, align>();

	BOOST_CHECK(!b);
}
#endif

BOOST_AUTO_TEST_CASE(buffer_128_a_4_succeeds)
{
	uint32_t const sz = 128, align = 4;
	buffer<sz, align>::ptr b = create_buffer<sz, align>();

	BOOST_CHECK(b);
}

#if COMPILE_TIME_ERROR
BOOST_AUTO_TEST_CASE(buffer_128_a_5_fails)
{
	uint32_t const sz = 128, align = 5;
	buffer<sz, align>::ptr b = create_buffer<sz, align>();

	BOOST_CHECK(!b);
}

BOOST_AUTO_TEST_CASE(buffer_128_a_6_fails)
{
	uint32_t const sz = 128, align = 6;
	buffer<sz, align>::ptr b = create_buffer<sz, align>();

	BOOST_CHECK(!b);
}

BOOST_AUTO_TEST_CASE(buffer_128_a_7_fails)
{
	uint32_t const sz = 128, align = 7;
	buffer<sz, align>::ptr b = create_buffer<sz, align>();

	BOOST_CHECK(!b);
}
#endif

// Varying buffer size, alignment 8
BOOST_AUTO_TEST_CASE(buffer_8_a_8)
{
	uint32_t const sz = 8, align = 8;
	buffer<sz, align>::ptr b = create_buffer<sz, align>();

	BOOST_CHECK_EQUAL((unsigned long) b->raw() & (align - 1), 0);
}

BOOST_AUTO_TEST_CASE(buffer_16_a_8)
{
	uint32_t const sz = 16, align = 8;
	buffer<sz, align>::ptr b = create_buffer<sz, align>();

	BOOST_CHECK_EQUAL((unsigned long) b->raw() & (align - 1), 0);
}

BOOST_AUTO_TEST_CASE(buffer_32_a_8)
{
	uint32_t const sz = 32, align = 8;
	buffer<sz, align>::ptr b = create_buffer<sz, align>();

	BOOST_CHECK_EQUAL((unsigned long) b->raw() & (align - 1), 0);
}

BOOST_AUTO_TEST_CASE(buffer_64_a_8)
{
	uint32_t const sz = 64, align = 8;
	buffer<sz, align>::ptr b = create_buffer<sz, align>();

	BOOST_CHECK_EQUAL((unsigned long) b->raw() & (align - 1), 0);
}

BOOST_AUTO_TEST_CASE(buffer_128_a_8)
{
	uint32_t const sz = 128, align = 8;
	buffer<sz, align>::ptr b = create_buffer<sz, align>();

	BOOST_CHECK_EQUAL((unsigned long) b->raw() & (align - 1), 0);
}

BOOST_AUTO_TEST_CASE(buffer_256_a_8)
{
	uint32_t const sz = 256, align = 8;
	buffer<sz, align>::ptr b = create_buffer<sz, align>();

	BOOST_CHECK_EQUAL((unsigned long) b->raw() & (align - 1), 0);
}

BOOST_AUTO_TEST_CASE(buffer_512_a_8)
{
	uint32_t const sz = 512, align = 8;
	buffer<sz, align>::ptr b = create_buffer<sz, align>();

	BOOST_CHECK_EQUAL((unsigned long) b->raw() & (align - 1), 0);
}

BOOST_AUTO_TEST_CASE(buffer_1024_a_8)
{
	uint32_t const sz = 1024, align = 8;
	buffer<sz, align>::ptr b = create_buffer<sz, align>();

	BOOST_CHECK_EQUAL((unsigned long) b->raw() & (align - 1), 0);
}

BOOST_AUTO_TEST_CASE(buffer_2048_a_8)
{
	uint32_t const sz = 2048, align = 8;
	buffer<sz, align>::ptr b = create_buffer<sz, align>();

	BOOST_CHECK_EQUAL((unsigned long) b->raw() & (align - 1), 0);
}

BOOST_AUTO_TEST_CASE(buffer_4096_a_8)
{
	uint32_t const sz = 4096, align = 8;
	buffer<sz, align>::ptr b = create_buffer<sz, align>();

	BOOST_CHECK_EQUAL((unsigned long) b->raw() & (align - 1), 0);
}

BOOST_AUTO_TEST_CASE(buffer_8192_a_8)
{
	uint32_t const sz = 8192, align = 8;
	buffer<sz, align>::ptr b = create_buffer<sz, align>();

	BOOST_CHECK_EQUAL((unsigned long) b->raw() & (align - 1), 0);
}


// Varying buffer size, alignment 16
BOOST_AUTO_TEST_CASE(buffer_8_a_16)
{
	uint32_t const sz = 8, align = 16;
	buffer<sz, align>::ptr b = create_buffer<sz, align>();

	BOOST_CHECK_EQUAL((unsigned long) b->raw() & (align - 1), 0);
}

BOOST_AUTO_TEST_CASE(buffer_16_a_16)
{
	uint32_t const sz = 16, align = 16;
	buffer<sz, align>::ptr b = create_buffer<sz, align>();

	BOOST_CHECK_EQUAL((unsigned long) b->raw() & (align - 1), 0);
}

BOOST_AUTO_TEST_CASE(buffer_32_a_16)
{
	uint32_t const sz = 32, align = 16;
	buffer<sz, align>::ptr b = create_buffer<sz, align>();

	BOOST_CHECK_EQUAL((unsigned long) b->raw() & (align - 1), 0);
}

BOOST_AUTO_TEST_CASE(buffer_64_a_16)
{
	uint32_t const sz = 64, align = 16;
	buffer<sz, align>::ptr b = create_buffer<sz, align>();

	BOOST_CHECK_EQUAL((unsigned long) b->raw() & (align - 1), 0);
}

BOOST_AUTO_TEST_CASE(buffer_128_a_16)
{
	uint32_t const sz = 128, align = 16;
	buffer<sz, align>::ptr b = create_buffer<sz, align>();

	BOOST_CHECK_EQUAL((unsigned long) b->raw() & (align - 1), 0);
}

BOOST_AUTO_TEST_CASE(buffer_256_a_16)
{
	uint32_t const sz = 256, align = 16;
	buffer<sz, align>::ptr b = create_buffer<sz, align>();

	BOOST_CHECK_EQUAL((unsigned long) b->raw() & (align - 1), 0);
}

BOOST_AUTO_TEST_CASE(buffer_512_a_16)
{
	uint32_t const sz = 512, align = 16;
	buffer<sz, align>::ptr b = create_buffer<sz, align>();

	BOOST_CHECK_EQUAL((unsigned long) b->raw() & (align - 1), 0);
}

BOOST_AUTO_TEST_CASE(buffer_1024_a_16)
{
	uint32_t const sz = 1024, align = 16;
	buffer<sz, align>::ptr b = create_buffer<sz, align>();

	BOOST_CHECK_EQUAL((unsigned long) b->raw() & (align - 1), 0);
}

BOOST_AUTO_TEST_CASE(buffer_2048_a_16)
{
	uint32_t const sz = 2048, align = 16;
	buffer<sz, align>::ptr b = create_buffer<sz, align>();

	BOOST_CHECK_EQUAL((unsigned long) b->raw() & (align - 1), 0);
}

BOOST_AUTO_TEST_CASE(buffer_4096_a_16)
{
	uint32_t const sz = 4096, align = 16;
	buffer<sz, align>::ptr b = create_buffer<sz, align>();

	BOOST_CHECK_EQUAL((unsigned long) b->raw() & (align - 1), 0);
}

BOOST_AUTO_TEST_CASE(buffer_8192_a_16)
{
	uint32_t const sz = 8192, align = 16;
	buffer<sz, align>::ptr b = create_buffer<sz, align>();

	BOOST_CHECK_EQUAL((unsigned long) b->raw() & (align - 1), 0);
}

#undef	COMPILE_TIME_ERROR

//----------------------------------------------------------------

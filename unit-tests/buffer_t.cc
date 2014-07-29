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

#include "gmock/gmock.h"

using namespace persistent_data;
using namespace testing;

// FIXME: get rid of these comments, the tests should be self explanatory

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

	template <typename buffer>
	void assert_aligned(buffer const &b) {
		ASSERT_THAT((unsigned long) b.raw() & (buffer::ALIGNMENT - 1), Eq(0u));
	}
}

//----------------------------------------------------------------

TEST(BufferTest, buffer_8_a_8_raw_access)
{
	uint32_t const sz = 8, align = 8;
	buffer<sz, align>::ptr b = create_buffer<sz, align>();
	unsigned char *p = b->raw();
	// unsigned char const *pc = b->raw();

	p[0] = 0;
	ASSERT_THAT(p[0], Eq(0));
	p[0] = 4;
	ASSERT_THAT(p[0], Eq(4));
}

TEST(BufferTest, buffer_8_a_8_access)
{
	uint32_t const sz = 8, align = 8;
	buffer<sz, align>::ptr b = create_buffer<sz, align>();

	(*b)[0] = 0;
	ASSERT_THAT((*b)[0], Eq(0));
}

TEST(BufferTest, buffer_8_a_8_oob)
{
	uint32_t const sz = 8, align = 8;
	buffer<sz, align>::ptr b = create_buffer<sz, align>();

	ASSERT_THROW((*b)[8], std::range_error);
}

TEST(BufferTest, buffer_128_a_2_succeeds)
{
	uint32_t const sz = 128, align = 2;
	buffer<sz, align>::ptr b = create_buffer<sz, align>();

	ASSERT_TRUE(static_cast<bool>(b));
}

TEST(BufferTest, buffer_128_a_4_succeeds)
{
	uint32_t const sz = 128, align = 4;
	buffer<sz, align>::ptr b = create_buffer<sz, align>();

	ASSERT_TRUE(static_cast<bool>(b));
}

// Varying buffer size, alignment 8
TEST(BufferTest, obeys_alignment)
{
	assert_aligned(*create_buffer<8, 8>());
	assert_aligned(*create_buffer<16, 8>());
	assert_aligned(*create_buffer<32, 8>());
	assert_aligned(*create_buffer<64, 8>());
	assert_aligned(*create_buffer<128, 8>());
	assert_aligned(*create_buffer<256, 8>());
	assert_aligned(*create_buffer<512, 8>());
	assert_aligned(*create_buffer<1024, 8>());
	assert_aligned(*create_buffer<2048, 8>());
	assert_aligned(*create_buffer<4096, 8>());
	assert_aligned(*create_buffer<8192, 8>());

	assert_aligned(*create_buffer<8, 16>());
	assert_aligned(*create_buffer<16, 16>());
	assert_aligned(*create_buffer<32, 16>());
	assert_aligned(*create_buffer<64, 16>());
	assert_aligned(*create_buffer<128, 16>());
	assert_aligned(*create_buffer<256, 16>());
	assert_aligned(*create_buffer<512, 16>());
	assert_aligned(*create_buffer<1024, 16>());
	assert_aligned(*create_buffer<2048, 16>());
	assert_aligned(*create_buffer<4096, 16>());
	assert_aligned(*create_buffer<8192, 16>());
}

#if 0
#if COMPILE_TIME_ERROR
TEST(BufferTest, buffer_copy_fails)
{
	uint32_t const sz = 8, align = 8;
	buffer<sz, align>::ptr b1 = create_buffer<sz, align>();
	buffer<sz, align>::ptr b2;

	*b2 = *b1; // Compile time failure
}
#endif

#if COMPILE_TIME_ERROR
TEST(BufferTest, buffer_8_a_8_raw_const_access)
{
	uint32_t const sz = 8, align = 8;
	buffer<sz, align>::ptr b = create_buffer<sz, align>();
	unsigned char const *pc = b->raw();

	pc[0] = 5; // Compile time error accessing read-only location
}
#endif

#if COMPILE_TIME_ERROR
TEST(BufferTest, buffer_8_a_8_const_access)
{
	uint32_t const sz = 8, align = 8;
	buffer<sz, align>::const_ptr b = create_buffer<sz, align>();

	(*b)[0] = 0; // Compile time error accessing read-only location 
}
#endif


// 8 byte buffer size, varying alignment from 1 - 7
#if COMPILE_TIME_ERROR
TEST(BufferTest, buffer_128_a_1_fails)
{
	uint32_t const sz = 128, align = 1;
	buffer<sz, align>::ptr b = create_buffer<sz, align>();

	BOOST_CHECK(!b);
	ASSERT_THAT((unsigned long) b->raw() & (align - 1), 1);
}
#endif

#if COMPILE_TIME_ERROR
TEST(BufferTest, buffer_128_a_3_fails)
{
	uint32_t const sz = 128, align = 3;
	buffer<sz, align>::ptr b = create_buffer<sz, align>();

	BOOST_CHECK(!b);
}
#endif

#if COMPILE_TIME_ERROR
TEST(BufferTest, buffer_128_a_5_fails)
{
	uint32_t const sz = 128, align = 5;
	buffer<sz, align>::ptr b = create_buffer<sz, align>();

	BOOST_CHECK(!b);
}

TEST(BufferTest, buffer_128_a_6_fails)
{
	uint32_t const sz = 128, align = 6;
	buffer<sz, align>::ptr b = create_buffer<sz, align>();

	BOOST_CHECK(!b);
}

TEST(BufferTest, buffer_128_a_7_fails)
{
	uint32_t const sz = 128, align = 7;
	buffer<sz, align>::ptr b = create_buffer<sz, align>();

	BOOST_CHECK(!b);
}
#endif


#undef	COMPILE_TIME_ERROR
#endif
//----------------------------------------------------------------

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

#ifndef BUFFER_H
#define BUFFER_H

#include <stdint.h>
// #include <stdlib.h>
#include <malloc.h>

#include <boost/noncopyable.hpp>
#include <boost/optional.hpp>
#include <boost/shared_ptr.hpp>
#include <boost/static_assert.hpp>

#include <stdexcept>

//----------------------------------------------------------------

namespace persistent_data {
	uint32_t const DEFAULT_BUFFER_SIZE = 4096;

	// Allocate buffer of Size with Alignment imposed.
	//
	// Allocation needs to be on the heap in order to provide alignment
	// guarantees.
	//
	// Alignment must be a power of two.
	template <uint32_t Size = DEFAULT_BUFFER_SIZE, uint32_t Alignment = 512>
	class buffer : private boost::noncopyable {
	public:
		BOOST_STATIC_ASSERT_MSG((Alignment > 1) & !(Alignment & (Alignment - 1)),
					"Alignment must be a power of two.");

		static uint32_t const ALIGNMENT = Alignment;
		typedef boost::shared_ptr<buffer> ptr;
		typedef boost::shared_ptr<buffer const> const_ptr;

		size_t size() const {
			return Size;
		}

		unsigned char &operator[](unsigned index) {
			check_index(index);

			return data_[index];
		}

		unsigned char const &operator[](unsigned index) const {
			check_index(index);

			return data_[index];
		}

		unsigned char *raw() {
			return data_;
		}

		unsigned char const *raw() const {
			return data_;
		}

		static void *operator new(size_t s) {
			// void *r;
			// return posix_memalign(&r, Alignment, s) ? NULL : r;

			// Allocates size bytes and returns a pointer to the
			// allocated memory. The memory address will be a
			// multiple of 'Alignment', which must be a power of two
			void *mem = memalign(Alignment, s);
			if (!mem)
				throw std::bad_alloc();

			return mem;
		}

		static void operator delete(void *p) {
			free(p);
		}

	private:
		unsigned char data_[Size];

		static void check_index(unsigned index) {
			if (index >= Size)
				throw std::range_error("buffer index out of bounds");
		}
	};
}

//----------------------------------------------------------------

#endif

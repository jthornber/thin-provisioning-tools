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
#include <stdlib.h>

#include <stdexcept>
#include <boost/noncopyable.hpp>

//----------------------------------------------------------------

namespace persistent_data {

	uint32_t const MD_BLOCK_SIZE = 4096;

	// Allocate buffer
	//
	// Allocation needs to be on the heap in order to provide alignment guarantees!
	// 
	// Alignment must be a power of two and a multiple of sizeof(void *)


	// Heinz: could you move this to a separate file.  Add a big
	// comment explaining that you should allocate it on the heap if
	// you want the alignment guarantees.  Then write a test suite
	// (buffer_t) that covers the following cases.
	//
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

	template <uint32_t BlockSize = MD_BLOCK_SIZE, uint32_t Alignment = 512>
	class buffer : private boost::noncopyable {
	public:
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
			void *r;

			return ::posix_memalign(&r, Alignment, s) ? r : (void*) NULL;
		}

		static void operator delete(void *p) {
			free(p);
		}

	private:
		unsigned char data_[BlockSize];

		static void check_index(unsigned index) {
			if (index >= BlockSize)
				throw std::runtime_error("buffer index out of bounds");
		}

	};
}

//----------------------------------------------------------------

#endif

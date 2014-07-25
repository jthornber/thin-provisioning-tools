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
#include <malloc.h>

#include <boost/optional.hpp>
#include <boost/shared_ptr.hpp>
#include <boost/static_assert.hpp>

#include <stdexcept>

//----------------------------------------------------------------

namespace persistent_data {
	uint32_t const DEFAULT_BUFFER_SIZE = 4096;

	template <uint32_t Size = DEFAULT_BUFFER_SIZE>
	class buffer {
	public:
		buffer(void *data, bool writeable = true)
			: data_(static_cast<unsigned char *>(data)),
			  writeable_(writeable) {
		}

		typedef boost::shared_ptr<buffer> ptr;
		typedef boost::shared_ptr<buffer const> const_ptr;

		size_t size() const {
			return Size;
		}

		unsigned char &operator[](unsigned index) {
			check_writeable();
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

		void set_data(void *data) {
			data_ = static_cast<unsigned char *>(data);
		}

	private:
		static void check_index(unsigned index) {
			if (index >= Size)
				throw std::range_error("buffer index out of bounds");
		}

		void check_writeable() const {
			if (!writeable_)
				throw std::runtime_error("buffer isn't writeable");
		}

		unsigned char *data_;
		bool writeable_;
	};
}

//----------------------------------------------------------------

#endif

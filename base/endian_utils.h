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

#ifndef BASE_ENDIAN_H
#define BASE_ENDIAN_H

#include <endian.h>
#include <stdint.h>
#include <boost/static_assert.hpp>

//----------------------------------------------------------------

namespace base {

	// These are just little wrapper types to make the compiler
	// understand that the le types are not assignable to the
	// corresponding cpu type.
	struct le16 {
		explicit le16(uint16_t v = 0)
			: v_(v) {
		}

		uint16_t v_;
	} __attribute__((packed));

	struct le32 {
		explicit le32(uint32_t v = 0)
			: v_(v) {
		}

		uint32_t v_;
	} __attribute__((packed));

	struct le64 {
		explicit le64(uint64_t v = 0)
			: v_(v) {
		}

		uint64_t v_;
	} __attribute__((packed));

	//--------------------------------

	// FIXME: actually do the conversions !
	template <typename CPUType, typename DiskType>
	CPUType	to_cpu(DiskType const &d) {
		BOOST_STATIC_ASSERT(sizeof(d) == 0);
	}

	template <typename DiskType, typename CPUType>
	DiskType to_disk(CPUType const &v) {
		BOOST_STATIC_ASSERT(sizeof(v) == 0);
	}

	template <>
	inline uint16_t to_cpu<uint16_t, le16>(le16 const &d) {
		return le16toh(d.v_);
	}

	template <>
	inline le16 to_disk<le16, uint16_t>(uint16_t const &v) {
		return le16(htole16(v));
	}

	template <>
	inline uint32_t to_cpu<uint32_t, le32>(le32 const &d) {
		return le32toh(d.v_);
	}

	template <>
	inline le32 to_disk<le32, uint32_t>(uint32_t const &v) {
		return le32(htole32(v));
	}

	template <>
	inline uint64_t to_cpu<uint64_t, le64>(le64 const &d) {
		return le64toh(d.v_);
	}

	template <>
	inline le64 to_disk<le64, uint64_t>(uint64_t const &v) {
		return le64(htole64(v));
	}

	//--------------------------------

	bool test_bit_le(void const *bits, unsigned b);
	void set_bit_le(void *bits, unsigned b);
	void clear_bit_le(void *bits, unsigned b);
}

//----------------------------------------------------------------

#endif

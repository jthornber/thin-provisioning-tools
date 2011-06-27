#ifndef ENDIAN_H
#define ENDIAN_H

#include <boost/static_assert.hpp>

//----------------------------------------------------------------

namespace base {

	// These are just little wrapper types to make the compiler
	// understand that the le types are not assignable to the
	// corresponding cpu type.

	struct __le16 {
		explicit __le16(uint16_t v = 0.0)
			: v_(v) {
		}

		uint16_t v_;
	} __attribute__((packed));

	struct __le32 {
		explicit __le32(uint32_t v = 0.0)
			: v_(v) {
		}

		uint32_t v_;
	} __attribute__((packed));

	struct __le64 {
		explicit __le64(uint64_t v = 0.0)
			: v_(v) {
		}

		uint64_t v_;
	} __attribute__((packed));

	//--------------------------------

	template <typename CPUType, typename DiskType>
	CPUType	to_cpu(DiskType const &d) {
		BOOST_STATIC_ASSERT(sizeof(d) == 0);
	}

	template <typename DiskType, typename CPUType>
	DiskType to_disk(CPUType const &v) {
		BOOST_STATIC_ASSERT(sizeof(v) == 0);
	}

	template <>
	uint16_t to_cpu<uint16_t, __le16>(__le16 const &d) {
		return d.v_;
	}

	template <>
	__le16 to_disk<__le16, uint16_t>(uint16_t const &v) {
		return __le16(v);
	}

	template <>
	uint32_t to_cpu<uint32_t, __le32>(__le32 const &d) {
		return d.v_;
	}

	template <>
	__le32 to_disk<__le32, uint32_t>(uint32_t const &v) {
		return __le32(v);
	}

	template <>
	uint64_t to_cpu<uint64_t, __le64>(__le64 const &d) {
		return d.v_;
	}

	template <>
	__le64 to_disk<__le64, uint64_t>(uint64_t const &v) {
		return __le64(v);
	}
}

//----------------------------------------------------------------

#endif

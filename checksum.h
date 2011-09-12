#ifndef CHECKSUM_H
#define CHECKSUM_H

#include <stdint.h>

//----------------------------------------------------------------

namespace base {
	class crc32 {
		crc32(uint32_t seed);

		void append(void const *buffer, unsigned len);
		uint32_t get_sum() const;

	private:
		uint32_t sum_;
	};
}

//----------------------------------------------------------------

#endif

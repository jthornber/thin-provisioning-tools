#ifndef CHECKSUM_H
#define CHECKSUM_H

#include <stdint.h>

//----------------------------------------------------------------

namespace base {
	class crc32c {
	public:
		crc32c(uint32_t xor_value);

		void append(void const *buffer, unsigned len);
		uint32_t get_sum() const;

	private:
		uint32_t xor_value_;
		uint32_t sum_;
	};
}

//----------------------------------------------------------------

#endif

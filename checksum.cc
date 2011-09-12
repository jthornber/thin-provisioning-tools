#include "checksum.h"

#include <zlib.h>

using namespace base;

//----------------------------------------------------------------

crc32::crc32(uint32_t seed)
	: sum_(seed) {
}

void
crc32::append(void const *buffer, unsigned len)
{
	sum_ = ::crc32(sum_, reinterpret_cast<Bytef const *>(buffer), len);
}

uint32_t
crc32::get_sum() const
{
	return sum_;
}

//----------------------------------------------------------------

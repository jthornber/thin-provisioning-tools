#include "checksum.h"

#include <boost/crc.hpp>

using namespace base;

//----------------------------------------------------------------

crc32c::crc32c(uint32_t xor_value)
	: xor_value_(xor_value),
	  sum_(0)
{
}

void
crc32c::append(void const *buffer, unsigned len)
{
	uint32_t const powers = 0x1EDC6F41;

	boost::crc_basic<32> crc(powers, 0xffffffff, 0, true, true);
	crc.process_bytes(buffer, len);
	sum_ = crc.checksum();
}

uint32_t
crc32c::get_sum() const
{
	return sum_ ^ xor_value_;
}

//----------------------------------------------------------------

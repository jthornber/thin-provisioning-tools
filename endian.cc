#include "endian.h"

using namespace base;

//----------------------------------------------------------------

bool
base::test_bit_le(void const *bits, unsigned b)
{
	__le64 const *w = reinterpret_cast<__le64 const *>(bits);
        w += b / 64;

        uint64_t v = to_cpu<uint64_t>(*w);
        uint64_t mask = 1;
        mask = mask << (b % 64);
        return (v & mask) ? true : false;
}

void
base::set_bit_le(void *bits, unsigned b)
{
	__le64 *w = reinterpret_cast<__le64 *>(bits);
	w += b / 64;

	uint64_t v = to_cpu<uint64_t>(*w);
	uint64_t mask = 1;
	mask = mask << (b % 64);
	v |= mask;
	*w = to_disk<__le64>(v);
}

void
base::clear_bit_le(void *bits, unsigned b)
{
	__le64 *w = reinterpret_cast<__le64 *>(bits);
	w += b / 64;

	uint64_t v = to_cpu<uint64_t>(*w);
	uint64_t mask = 1;
	mask = mask << (b % 64);
	mask = ~mask;
	v &= mask;
	*w = to_disk<__le64>(v);
}

//----------------------------------------------------------------

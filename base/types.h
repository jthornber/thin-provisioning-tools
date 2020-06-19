#ifndef BASE_TYPES_H
#define BASE_TYPES_H

#include <stdint.h>

//----------------------------------------------------------------

namespace base {
	using sector_t = uint64_t;
	unsigned const SECTOR_SHIFT = 9;
}

//----------------------------------------------------------------

#endif

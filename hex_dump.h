#ifndef HEX_DUMP_H
#define HEX_DUMP_H

#include <iosfwd>

//----------------------------------------------------------------

namespace base {
	void hex_dump(std::ostream &out, void const *data, size_t len);
}

//----------------------------------------------------------------

#endif

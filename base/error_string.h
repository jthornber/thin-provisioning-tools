#ifndef BASE_ERROR_STRING_H
#define BASE_ERROR_STRING_H

#include <string>

//----------------------------------------------------------------

namespace base {
	// There are a couple of version of strerror_r kicking around, so
	// we wrap it.
	std::string error_string(int err);
}

//----------------------------------------------------------------

#endif

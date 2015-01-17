#include "base/error_string.h"

#include <errno.h>
#include <stdexcept>

#include <string.h>

using namespace std;

//----------------------------------------------------------------

#ifdef STRERROR_R_CHAR_P

string base::error_string(int err)
{
	char *ptr;
	char buffer[128];

	ptr = strerror_r(errno, buffer, sizeof(buffer));
	return string(ptr);
}

#else

string base::error_string(int err)
{
	int r;
	char buffer[128];

	r = strerror_r(errno, buffer, sizeof(buffer));
	if (r)
		throw runtime_error("strerror_r failed");

	return string(buffer);
}

#endif

//----------------------------------------------------------------

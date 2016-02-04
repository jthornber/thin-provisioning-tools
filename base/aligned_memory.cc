#include "base/aligned_memory.h"

#include <stdlib.h>
#include <stdexcept>

using namespace base;

//----------------------------------------------------------------

aligned_memory::aligned_memory(size_t len, size_t alignment)
{
	int r = posix_memalign(&data_, alignment, len);
	if (r)
		throw std::runtime_error("couldn't allocate aligned memory");
}

aligned_memory::~aligned_memory()
{
	free(data_);
}

//----------------------------------------------------------------

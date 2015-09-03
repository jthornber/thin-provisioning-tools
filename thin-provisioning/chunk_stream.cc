#include "thin-provisioning/chunk_stream.h"

using namespace std;
using namespace thin_provisioning;

//----------------------------------------------------------------

uint8_t
chunk::operator[](uint64_t n) const
{
	std::deque<mem>::const_iterator it;
	for (it = mem_.begin(); it != mem_.end(); it++) {
		uint64_t mem_len = it->end - it->begin;
		if (n > mem_len)
			n -= mem_len;
		else
			return it->begin[n];
	}

	throw runtime_error("chunk out of bounds");
}

//----------------------------------------------------------------

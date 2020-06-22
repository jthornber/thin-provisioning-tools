#ifndef BASE_IO_H
#define BASE_IO_H

#include "base/types.h"
#include <stdint.h>

//----------------------------------------------------------------

namespace base {
	enum req_op {
		REQ_OP_READ,
		REQ_OP_WRITE,
		REQ_OP_DISCARD
	};

	struct io {
		unsigned op_;
		sector_t sector_;
		sector_t size_;
	};
}

//----------------------------------------------------------------

#endif

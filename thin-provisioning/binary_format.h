#ifndef BINARY_H
#define BINARY_H

#include "emitter.h"

#include <iosfwd>

//----------------------------------------------------------------

namespace thin_provisioning {
	emitter::ptr create_binary_emitter(std::ostream &out);
}

//----------------------------------------------------------------

#endif

#ifndef HUMAN_READABLE_H
#define HUMAN_READABLE_H

#include "emitter.h"

#include <iosfwd>

//----------------------------------------------------------------

namespace thin_provisioning {
	emitter::ptr create_human_readable_emitter(std::ostream &out);
}

//----------------------------------------------------------------

#endif

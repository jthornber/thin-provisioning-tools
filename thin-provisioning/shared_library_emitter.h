#ifndef THIN_PROVISIONING_SHARED_LIBRARY_EMITTER_H
#define THIN_PROVISIONING_SHARED_LIBRARY_EMITTER_H

#include "thin-provisioning/emitter.h"

//----------------------------------------------------------------

namespace thin_provisioning {
	emitter::ptr create_custom_emitter(std::string const &shared_lib, std::ostream &out);
}

//----------------------------------------------------------------

#endif

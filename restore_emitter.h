#ifndef RESTORE_EMITTER_H
#define RESTORE_EMITTER_H

#include "emitter.h"
#include "metadata.h"

//----------------------------------------------------------------

namespace thin_provisioning {
	emitter::ptr create_restore_emitter(metadata::ptr md);
}

//----------------------------------------------------------------

#endif

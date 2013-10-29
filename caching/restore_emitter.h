#ifndef CACHE_RESTORE_EMITTER_H
#define CACHE_RESTORE_EMITTER_H

#include "emitter.h"
#include "metadata.h"

//----------------------------------------------------------------

namespace caching {
	emitter::ptr create_restore_emitter(metadata::ptr md, bool clean_shutdown = true);
}

//----------------------------------------------------------------

#endif

#ifndef CACHE_RESTORE_EMITTER_H
#define CACHE_RESTORE_EMITTER_H

#include "emitter.h"
#include "metadata.h"

//----------------------------------------------------------------

namespace caching {

	enum shutdown_type {
		CLEAN_SHUTDOWN,
		NO_CLEAN_SHUTDOWN
	};

	emitter::ptr create_restore_emitter(metadata::ptr md,
					    unsigned metadata_version,
					    shutdown_type st = CLEAN_SHUTDOWN);
}

//----------------------------------------------------------------

#endif

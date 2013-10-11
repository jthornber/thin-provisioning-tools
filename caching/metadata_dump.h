#ifndef CACHING_METADATA_DUMP_H
#define CACHING_METADATA_DUMP_H

#include "caching/metadata.h"
#include "caching/emitter.h"

//----------------------------------------------------------------

namespace caching {
	// If 'repair' is set then any metadata damage will be stepped
	// around.  Otherwise an exception will be thrown.
	void metadata_dump(metadata::ptr md, emitter::ptr out, bool repair);
}

//----------------------------------------------------------------

#endif

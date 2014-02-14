#ifndef ERA_METADATA_DUMP_H
#define ERA_METADATA_DUMP_H

#include "era/metadata.h"
#include "era/emitter.h"

//----------------------------------------------------------------

namespace era {
	void metadata_dump(metadata::ptr md, emitter::ptr out,
			   bool repair, bool logical);
}

//----------------------------------------------------------------

#endif

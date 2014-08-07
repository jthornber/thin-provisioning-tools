#ifndef ERA_RESTORE_EMITTER_H
#define ERA_RESTORE_EMITTER_H

#include "era/emitter.h"
#include "era/metadata.h"

//----------------------------------------------------------------

namespace era {
	emitter::ptr create_restore_emitter(metadata &md);
}

//----------------------------------------------------------------

#endif

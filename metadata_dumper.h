#ifndef METADATA_DUMPER_H
#define METADATA_DUMPER_H

#include "emitter.h"
#include "metadata.h"

//----------------------------------------------------------------

namespace thin_provisioning {
	void metadata_dump(metadata::ptr md, emitter::ptr e);
}

//----------------------------------------------------------------

#endif

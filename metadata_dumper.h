#ifndef METADATA_DUMPER_H
#define METADATA_DUMPER_H

#include "emitter.h"
#include "metadata_ll.h"

//----------------------------------------------------------------

namespace thin_provisioning {
	void metadata_dump(metadata_ll::ptr md, emitter::ptr e);
}

//----------------------------------------------------------------

#endif

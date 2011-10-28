#ifndef METADATA_CHECKER_H
#define METADATA_CHECKER_H

#include "error_set.h"
#include "metadata.h"

//----------------------------------------------------------------

namespace thin_provisioning {
	boost::optional<persistent_data::error_set::ptr> metadata_check(metadata::ptr md);
}

//----------------------------------------------------------------

#endif

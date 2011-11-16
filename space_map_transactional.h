#ifndef SPACE_MAP_TRANSACTIONAL_H
#define SPACE_MAP_TRANSACTIONAL_H

#include "space_map.h"

//----------------------------------------------------------------

namespace persistent_data {
	
	// FIXME: change name 'transactional' is so vague.

	// This space map ensures no blocks are allocated which have been
	// freed within the current transaction.
	checked_space_map::ptr create_transactional_sm(checked_space_map::ptr sm);
}

//----------------------------------------------------------------

#endif

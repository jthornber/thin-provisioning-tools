#ifndef CACHING_COMMANDS_H
#define CACHING_COMMANDS_H

#include "base/application.h"

//----------------------------------------------------------------

namespace caching {
	extern base::command cache_check_cmd;
	extern base::command cache_dump_cmd;
	extern base::command cache_metadata_size_cmd;
	extern base::command cache_restore_cmd;
	extern base::command cache_repair_cmd;
}

//----------------------------------------------------------------

#endif

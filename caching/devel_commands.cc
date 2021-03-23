#include "caching/commands.h"

using namespace base;
using namespace caching;

//----------------------------------------------------------------

void
caching::register_cache_commands(application &app)
{
	app.add_cmd(command::ptr(new cache_debug_cmd));
}

//----------------------------------------------------------------

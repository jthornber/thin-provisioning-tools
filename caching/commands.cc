#include "caching/commands.h"

using namespace base;
using namespace caching;

//----------------------------------------------------------------

void
caching::register_cache_commands(application &app)
{
	app.add_cmd(command::ptr(new cache_check_cmd));
	app.add_cmd(command::ptr(new cache_dump_cmd));
	app.add_cmd(command::ptr(new cache_metadata_size_cmd));
	app.add_cmd(command::ptr(new cache_restore_cmd));
	app.add_cmd(command::ptr(new cache_repair_cmd));
}

//----------------------------------------------------------------

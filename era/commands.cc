#include "era/commands.h"

using namespace base;

//----------------------------------------------------------------

void
era::register_era_commands(base::application &app)
{
	app.add_cmd(command::ptr(new era_check_cmd()));
	app.add_cmd(command::ptr(new era_dump_cmd()));
	app.add_cmd(command::ptr(new era_invalidate_cmd()));
	app.add_cmd(command::ptr(new era_restore_cmd()));
}

//----------------------------------------------------------------

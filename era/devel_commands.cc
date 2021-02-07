#include "era/commands.h"

using namespace base;
using namespace era;

//----------------------------------------------------------------

void
era::register_era_commands(base::application &app)
{
	app.add_cmd(command::ptr(new era_debug_cmd));
}

//----------------------------------------------------------------

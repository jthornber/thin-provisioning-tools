#include "cruncher/commands.h"

using namespace base;
using namespace cruncher;

//----------------------------------------------------------------

void
cruncher::register_crunch_commands(base::application &app)
{
	app.add_cmd(command::ptr(new crunch_cmd()));
}

//----------------------------------------------------------------

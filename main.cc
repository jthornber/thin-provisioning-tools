#include <iostream>

#include "base/application.h"

#include "caching/commands.h"
#include "era/commands.h"
#include "thin-provisioning/commands.h"

//----------------------------------------------------------------

int main(int argc, char **argv)
{
	using namespace base;

	application app;

	caching::register_cache_commands(app);
	era::register_era_commands(app);
	thin_provisioning::register_thin_commands(app);

	return app.run(argc, argv);
}

//----------------------------------------------------------------

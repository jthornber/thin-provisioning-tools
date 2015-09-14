#include <iostream>

#include "base/application.h"

#include "caching/commands.h"
#include "era/commands.h"
#include "thin-provisioning/commands.h"
#include "cruncher/commands.h"

//----------------------------------------------------------------

int main(int argc, char **argv)
{
	using namespace base;

	application app;

	app.add_cmd(caching::cache_check_cmd);
	app.add_cmd(caching::cache_dump_cmd);
	app.add_cmd(caching::cache_metadata_size_cmd);
	app.add_cmd(caching::cache_restore_cmd);
	app.add_cmd(caching::cache_repair_cmd);

	app.add_cmd(era::era_check_cmd);
	app.add_cmd(era::era_dump_cmd);
	app.add_cmd(era::era_invalidate_cmd);
	app.add_cmd(era::era_restore_cmd);

	app.add_cmd(thin_provisioning::thin_check_cmd);
	app.add_cmd(thin_provisioning::thin_delta_cmd);
	app.add_cmd(thin_provisioning::thin_dump_cmd);
	app.add_cmd(thin_provisioning::thin_metadata_size_cmd);
	app.add_cmd(thin_provisioning::thin_restore_cmd);
	app.add_cmd(thin_provisioning::thin_repair_cmd);
	app.add_cmd(thin_provisioning::thin_rmap_cmd);
	app.add_cmd(thin_provisioning::thin_show_dups_cmd);

	// FIXME: convert thin_metadata_size to c++
	//app.add_cmd(thin_provisioning::thin_metadata_size_cmd);

	app.add_cmd(cruncher::crunch_cmd);

	return app.run(argc, argv);
}

//----------------------------------------------------------------

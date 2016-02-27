#include "thin-provisioning/commands.h"

using namespace base;
using namespace thin_provisioning;

//----------------------------------------------------------------

void
thin_provisioning::register_thin_commands(base::application &app)
{
	app.add_cmd(command::ptr(new thin_check_cmd()));
	app.add_cmd(command::ptr(new thin_delta_cmd()));
	app.add_cmd(command::ptr(new thin_dump_cmd()));
	app.add_cmd(command::ptr(new thin_ls_cmd()));
	app.add_cmd(command::ptr(new thin_metadata_size_cmd()));
	app.add_cmd(command::ptr(new thin_restore_cmd()));
	app.add_cmd(command::ptr(new thin_repair_cmd()));
	app.add_cmd(command::ptr(new thin_rmap_cmd()));
	app.add_cmd(command::ptr(new thin_scan_cmd()));
	app.add_cmd(command::ptr(new thin_trim_cmd()));
}

//----------------------------------------------------------------

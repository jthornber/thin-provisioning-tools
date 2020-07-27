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
	app.add_cmd(command::ptr(new thin_trim_cmd()));

#ifdef DEV_TOOLS
	app.add_cmd(command::ptr(new thin_ll_dump_cmd()));
	app.add_cmd(command::ptr(new thin_ll_restore_cmd()));
	app.add_cmd(command::ptr(new thin_scan_cmd()));
	app.add_cmd(command::ptr(new thin_generate_damage_cmd()));
	app.add_cmd(command::ptr(new thin_generate_metadata_cmd()));
	app.add_cmd(command::ptr(new thin_generate_mappings_cmd()));
	app.add_cmd(command::ptr(new thin_show_duplicates_cmd()));
	app.add_cmd(command::ptr(new thin_show_metadata_cmd()));
	app.add_cmd(command::ptr(new thin_journal_cmd()));
#endif
}

//----------------------------------------------------------------

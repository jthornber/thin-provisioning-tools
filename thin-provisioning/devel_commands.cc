#include "thin-provisioning/commands.h"

using namespace base;
using namespace thin_provisioning;

//----------------------------------------------------------------

void
thin_provisioning::register_thin_commands(base::application &app)
{
	app.add_cmd(command::ptr(new thin_generate_damage_cmd()));
	app.add_cmd(command::ptr(new thin_generate_mappings_cmd()));
	app.add_cmd(command::ptr(new thin_generate_metadata_cmd()));
	app.add_cmd(command::ptr(new thin_journal_cmd()));
	app.add_cmd(command::ptr(new thin_ll_dump_cmd()));
	app.add_cmd(command::ptr(new thin_ll_restore_cmd()));
	app.add_cmd(command::ptr(new thin_patch_superblock_cmd()));
	app.add_cmd(command::ptr(new thin_scan_cmd()));
	app.add_cmd(command::ptr(new thin_show_duplicates_cmd()));
	app.add_cmd(command::ptr(new thin_show_metadata_cmd()));
}

//----------------------------------------------------------------

#ifndef THIN_PROVISIONING_COMMANDS_H
#define THIN_PROVISIONING_COMMANDS_H

#include "base/application.h"

//----------------------------------------------------------------

namespace thin_provisioning {
	extern base::command thin_check_cmd;
	extern base::command thin_delta_cmd;
	extern base::command thin_dump_cmd;
	extern base::command thin_metadata_size_cmd;
	extern base::command thin_restore_cmd;
	extern base::command thin_repair_cmd;
	extern base::command thin_rmap_cmd;
	extern base::command thin_trim_cmd;
	extern base::command thin_metadata_size_cmd;
	extern base::command thin_show_dups_cmd;
}

//----------------------------------------------------------------

#endif

#ifndef DBG_COMMANDS_H
#define DBG_COMMANDS_H

#include "dbg-lib/command_interpreter.h"
#include "dbg-lib/block_dumper.h"
#include "persistent-data/block.h"

//----------------------------------------------------------------

namespace dbg {
	dbg::command::ptr
	create_hello_handler();

	dbg::command::ptr
	create_exit_handler(dbg::command_interpreter::ptr interp);

	dbg::command::ptr
	create_block_handler(persistent_data::block_manager::ptr bm,
			     dbg::block_dumper::ptr dumper);
}

//----------------------------------------------------------------

#endif

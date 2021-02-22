#include "dbg-lib/commands.h"
#include "persistent-data/block.h"

#include <boost/lexical_cast.hpp>
#include <stdexcept>

using namespace dbg;
using namespace persistent_data;
using namespace std;

//----------------------------------------------------------------

namespace {
	class hello : public dbg::command {
		virtual void exec(dbg::strings const &args, ostream &out) {
			out << "Hello, world!" << endl;
		}
	};

	class exit_handler : public dbg::command {
	public:
		exit_handler(command_interpreter::ptr interpreter)
			: interpreter_(interpreter) {
		}

		virtual void exec(dbg::strings const &args, ostream &out) {
			out << "Goodbye!" << endl;
			interpreter_->exit_main_loop();
		}

	private:
		command_interpreter::ptr interpreter_;
	};

	class block_handler : public dbg::command {
	public:
		block_handler(block_manager::ptr bm, block_dumper::ptr dumper)
			: bm_(bm), dumper_(dumper) {
		}

		virtual void exec(dbg::strings const &args, ostream &out) {
			if (args.size() != 2)
				throw runtime_error("incorrect number of arguments");

			block_address block = boost::lexical_cast<block_address>(args[1]);

			// no checksum validation for debugging purpose
			block_manager::read_ref rr = bm_->read_lock(block);

			dumper_->show(rr, out);
		}

	private:
		block_manager::ptr bm_;
		block_dumper::ptr dumper_;
	};
}

//----------------------------------------------------------------

command::ptr
dbg::create_hello_handler()
{
	return command::ptr(new hello());
}

command::ptr
dbg::create_exit_handler(command_interpreter::ptr interp)
{
	return command::ptr(new exit_handler(interp));
}

command::ptr
dbg::create_block_handler(block_manager::ptr bm, block_dumper::ptr dumper)
{
	return command::ptr(new block_handler(bm, dumper));
}

//----------------------------------------------------------------

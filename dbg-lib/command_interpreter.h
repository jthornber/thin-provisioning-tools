#ifndef DBG_COMMAND_INTERPRETER
#define DBG_COMMAND_INTERPRETER

#include <boost/algorithm/string/classification.hpp>
#include <boost/algorithm/string/split.hpp>

#include <iostream>
#include <map>
#include <string>
#include <vector>

//----------------------------------------------------------------

namespace dbg {
	typedef std::vector<std::string> strings;

	class command {
	public:
		typedef std::shared_ptr<command> ptr;

		virtual ~command() {}
		virtual void exec(strings const &args, std::ostream &out) = 0;
	};

	class command_interpreter {
	public:
		typedef std::shared_ptr<command_interpreter> ptr;

		virtual void register_command(std::string const &str, command::ptr cmd) = 0;
		virtual void enter_main_loop() = 0;
		virtual void exit_main_loop() = 0;
	};

	command_interpreter::ptr
	create_command_interpreter(std::istream &in, std::ostream &out);
}

//----------------------------------------------------------------

#endif

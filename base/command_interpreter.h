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

		command_interpreter(std::istream &in, std::ostream &out)
			: in_(in),
			  out_(out),
			  exit_(false) {
		}

		void register_command(std::string const &str, command::ptr cmd) {
			commands_.insert(make_pair(str, cmd));
		}

		void enter_main_loop() {
			while (!exit_)
				do_once();
		}

		void exit_main_loop() {
			exit_ = true;
		}

	private:
		void do_once();

		std::istream &in_;
		std::ostream &out_;
		std::map <std::string, command::ptr> commands_;
		bool exit_;
	};
}

//----------------------------------------------------------------

#endif

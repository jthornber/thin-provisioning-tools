#include "base/command_interpreter.h"

using namespace dbg;
using namespace std;

//----------------------------------------------------------------

namespace {
	class command_interpreter_impl : public command_interpreter {
	public:
		typedef std::shared_ptr<command_interpreter> ptr;

		command_interpreter_impl(std::istream &in, std::ostream &out)
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

	//--------------------------------

	strings read_input(std::istream &in)
	{
		using namespace boost::algorithm;

		std::string input;
		getline(in, input);

		strings toks;
		split(toks, input, is_any_of(" \t"), token_compress_on);

		return toks;
	}
}

//----------------------------------------------------------------

void command_interpreter_impl::do_once()
{
	if (in_.eof())
		throw runtime_error("input closed");

	out_ << "> ";
	strings args = read_input(in_);

	std::map<std::string, command::ptr>::iterator it;
	it = commands_.find(args[0]);
	if (it == commands_.end())
		out_ << "Unrecognised command" << endl;
	else {
		try {
			it->second->exec(args, out_);
		} catch (std::exception &e) {
			cerr << e.what() << endl;
		}
	}
}

//----------------------------------------------------------------

command_interpreter::ptr
dbg::create_command_interpreter(std::istream &in, std::ostream &out)
{
	return command_interpreter::ptr(new command_interpreter_impl(in, out));
}

//----------------------------------------------------------------

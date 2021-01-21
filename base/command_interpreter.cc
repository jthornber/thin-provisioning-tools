#include "base/command_interpreter.h"

using namespace dbg;
using namespace std;

//----------------------------------------------------------------

namespace {
	strings read_input(std::istream &in) {
		using namespace boost::algorithm;

		std::string input;
		getline(in, input);

		strings toks;
		split(toks, input, is_any_of(" \t"), token_compress_on);

		return toks;
	}
}

//----------------------------------------------------------------

void command_interpreter::do_once() {
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

#include "base/application.h"

#include <boost/lexical_cast.hpp>
#include <libgen.h>
#include <linux/limits.h>
#include <string.h>
#include <stdlib.h>

using namespace base;
using namespace boost;
using namespace std;

//----------------------------------------------------------------

command::command(string const &name)
	: name_(name) {
}

void
command::die(string const &msg)
{
	cerr << msg << endl;
	usage(cerr);
	exit(1);
}

uint64_t
command::parse_uint64(string const &str, string const &desc)
{
	try {
		// FIXME: check trailing garbage is handled
		return lexical_cast<uint64_t>(str);

	} catch (...) {
		ostringstream out;
		out << "Couldn't parse " << desc << ": '" << str << "'";
		die(out.str());
	}

	return 0; // never get here
}

//----------------------------------------------------------------

int
application::run(int argc, char **argv)
{
	string cmd = get_basename(argv[0]);

	if (cmd == string("pdata_tools")) {
		argc--;
		argv++;

		if (!argc) {
			usage();
			return 1;
		}

		cmd = argv[0];
	}

	std::list<command::ptr>::const_iterator it;
	for (it = cmds_.begin(); it != cmds_.end(); ++it) {
		if (cmd == (*it)->get_name()) {
			try {
				return (*it)->run(argc, argv);
			} catch (std::exception const &e) {
				cerr << e.what() << "\n";
				return 1;
			}
		}
	}

	std::cerr << "Unknown command '" << cmd << "'\n";
	usage();
	return 1;
}

void
application::usage()
{
	std::cerr << "Usage: <command> <args>\n"
		  << "commands:\n";

	std::list<command::ptr>::const_iterator it;
	for (it = cmds_.begin(); it != cmds_.end(); ++it) {
		std::cerr << "  " << (*it)->get_name() << "\n";
	}
}

std::string
application::get_basename(std::string const &path) const
{
	char buffer[PATH_MAX + 1];

	memset(buffer, 0, sizeof(buffer));
	strncpy(buffer, path.c_str(), PATH_MAX);

	return ::basename(buffer);
}

//----------------------------------------------------------------

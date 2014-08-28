#include "base/application.h"

#include <linux/limits.h>
#include <string.h>

using namespace base;
using namespace std;

//----------------------------------------------------------------

int
application::run(int argc, char **argv)
{
	string cmd = basename(argv[0]);

	if (cmd == string("pdata_tools")) {
		argc--;
		argv++;

		if (!argc) {
			usage();
			return 1;
		}

		cmd = argv[0];
	}

	std::list<command const *>::const_iterator it;
	for (it = cmds_.begin(); it != cmds_.end(); ++it) {
		if (cmd == (*it)->get_name())
			return (*it)->run(argc, argv);
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

	std::list<command const *>::const_iterator it;
	for (it = cmds_.begin(); it != cmds_.end(); ++it) {
		std::cerr << "  " << (*it)->get_name() << "\n";
	}
}

std::string
application::basename(std::string const &path) const
{
	char buffer[PATH_MAX + 1];

	memset(buffer, 0, sizeof(buffer));
	strncpy(buffer, path.c_str(), PATH_MAX);

	return ::basename(buffer);
}

//----------------------------------------------------------------

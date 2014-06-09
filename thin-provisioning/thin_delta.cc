#include <getopt.h>
#include <iostream>
#include <libgen.h>

#include "version.h"

using namespace std;

//----------------------------------------------------------------

namespace {
	void usage(ostream &out, string const &cmd) {
		out << "Usage: " << cmd << " [options]" << endl
		    << "Options:" << endl
		    << "  {-h|--help}" << endl
		    << "  {-V|--version}" << endl;
	}
}

//----------------------------------------------------------------

int main(int argc, char **argv)
{
	int c;
	char const shortopts[] = "hV";
	option const longopts[] = {
		{ "help", no_argument, NULL, 'h' },
		{ "version", no_argument, NULL, 'V' }
	};

	while ((c = getopt_long(argc, argv, shortopts, longopts, NULL)) != -1) {
		switch (c) {
		case 'h':
			usage(cout, basename(argv[0]));
			return 0;

		case 'V':
			cout << THIN_PROVISIONING_TOOLS_VERSION << endl;
			return 0;
		}
	}

	return 0;
}

//----------------------------------------------------------------

#include <getopt.h>
#include <libgen.h>
#include <iostream>

#include "version.h"

using namespace std;

//----------------------------------------------------------------

namespace {
	void usage(ostream &out, string const &cmd) {
		out << "Usage: " << cmd << " [options] {device|file}" << endl
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
		{ "help", no_argument, NULL, 'h'},
		{ "version", no_argument, NULL, 'V'},
		{ NULL, no_argument, NULL, 0 }
	};

	while ((c = getopt_long(argc, argv, shortopts, longopts, NULL)) != -1) {
		switch(c) {
		case 'h':
			usage(cout, basename(argv[0]));
			return 0;

		case 'V':
			cout << THIN_PROVISIONING_TOOLS_VERSION << endl;
			return 0;

		default:
			usage(cerr, basename(argv[0]));
			return 1;
		}
	}

#if 0
	if (argc == optind) {
		cerr << "No input file provided." << endl;
		usage(cerr, basename(argv[0]));
		return 1;
	}
#endif

	return 0;
}

//----------------------------------------------------------------

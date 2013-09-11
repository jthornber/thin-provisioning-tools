#include <fstream>
#include <getopt.h>
#include <libgen.h>
#include <iostream>

#include "version.h"
#include "caching/xml_format.h"

using namespace std;
using namespace caching;

//----------------------------------------------------------------

namespace {
	string const STDOUT_PATH("-");

	bool want_stdout(string const &output) {
		return output == STDOUT_PATH;
	}

	int dump_(string const &dev, ostream &out) {
		emitter::ptr e = create_xml_emitter(out);
		return 0;
	}

	int dump(string const &dev, string const &output) {
		if (want_stdout(output)) {
			ofstream out(output.c_str());
			return dump_(dev, out);
		} else
			return dump_(dev, cout);
	}

	void usage(ostream &out, string const &cmd) {
		out << "Usage: " << cmd << " [options] {device|file}" << endl
		    << "Options:" << endl
		    << "  {-h|--help}" << endl
		    << "  {-o <xml file>}" << endl
		    << "  {-V|--version}" << endl;
	}
}

//----------------------------------------------------------------

int main(int argc, char **argv)
{
	int c;
	string output("-");
	char const shortopts[] = "ho:V";

	option const longopts[] = {
		{ "help", no_argument, NULL, 'h'},
		{ "output", required_argument, NULL, 'o'},
		{ "version", no_argument, NULL, 'V'},
		{ NULL, no_argument, NULL, 0 }
	};

	while ((c = getopt_long(argc, argv, shortopts, longopts, NULL)) != -1) {
		switch(c) {
		case 'h':
			usage(cout, basename(argv[0]));
			return 0;

		case 'o':
			output = optarg;
			break;

		case 'V':
			cout << THIN_PROVISIONING_TOOLS_VERSION << endl;
			return 0;

		default:
			usage(cerr, basename(argv[0]));
			return 1;
		}
	}

	if (argc == optind) {
		cerr << "No input file provided." << endl;
		usage(cerr, basename(argv[0]));
		return 1;
	}

	return dump(argv[optind], output);
}

//----------------------------------------------------------------

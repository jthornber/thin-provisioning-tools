#include <iostream>
#include <getopt.h>
#include <libgen.h>
#include <sstream>
#include <vector>

#include "version.h"

#include "persistent-data/range.h"
#include "thin-provisioning/superblock.h"
#include "thin-provisioning/mapping_tree.h"

using namespace std;
using namespace thin_provisioning;

//----------------------------------------------------------------

namespace {
	typedef range<block_address> region;

	int rmap(string const &path, vector<region> const &regions) {
		cerr << "Not implemented" << endl;
		return 0;
	}

	region parse_region(string const &str) {
		istringstream in(str);

		char dots[2] = {'\0', '\0'};
		block_address begin, end;

		in >> begin;
		in.read(dots, sizeof(dots));
		if (dots[0] != '.' || dots[1] != '.')
			throw runtime_error("badly formed region (no dots)");
		in >> end;

		if (in.fail())
			throw runtime_error("badly formed region (couldn't parse numbers)");

		if (end <= begin)
			throw runtime_error("badly formed region (end <= begin)");

		return region(begin, end);
	};

	void usage(ostream &out, string const &cmd) {
		out << "Usage: " << cmd << " [options] {device|file}" << endl
		    << "Options:" << endl
		    << "  {-h|--help}" << endl
		    << "  {-V|--version}" << endl
		    << "  {--region <block range>}*" << endl
		    << "Where:" << endl
		    << "  <block range> is of the form <begin>..<one-past-the-end>" << endl
		    << "  for example 5..45 denotes blocks 5 to 44 inclusive, but not block 45" << endl;
	}
}

//----------------------------------------------------------------

int main(int argc, char **argv)
{
	int c;
	vector<region> regions;
	char const shortopts[] = "hV";
	option const longopts[] = {
		{ "help", no_argument, NULL, 'h'},
		{ "version", no_argument, NULL, 'V'},
		{ "region", required_argument, NULL, 1},
		{ NULL, no_argument, NULL, 0 }
	};

	while ((c = getopt_long(argc, argv, shortopts, longopts, NULL)) != -1) {
		switch (c) {
		case 'h':
			usage(cout, basename(argv[0]));
			return 0;

		case 'V':
			cout << THIN_PROVISIONING_TOOLS_VERSION << endl;
			return 0;

		case 1:
			// region
			regions.push_back(parse_region(optarg));
			break;

		default:
			usage(cerr, basename(argv[0]));
			return 1;
		}
	}

	if (argc == optind) {
		cerr << "No input file provided." << endl;
		usage(cerr, basename(argv[0]));
		exit(1);
	}

	return rmap(argv[optind], regions);
}

//----------------------------------------------------------------

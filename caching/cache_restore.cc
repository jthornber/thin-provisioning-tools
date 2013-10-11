#include "version.h"

#include "caching/metadata.h"
#include "caching/restore_emitter.h"
#include "caching/xml_format.h"
#include "persistent-data/file_utils.h"

#include <fstream>
#include <getopt.h>
#include <iostream>
#include <libgen.h>
#include <string>

using namespace caching;
using namespace persistent_data;
using namespace std;

//----------------------------------------------------------------

namespace {
	int restore(string const &xml_file, string const &dev) {
		try {
			block_manager<>::ptr bm = open_bm(dev, block_io<>::READ_WRITE);
			metadata::ptr md(new metadata(bm, metadata::CREATE));
			emitter::ptr restorer = create_restore_emitter(md);

			check_file_exists(xml_file);
			ifstream in(xml_file.c_str(), ifstream::in);
			parse_xml(in, restorer);

		} catch (std::exception &e) {
			cerr << e.what() << endl;
			return 1;
		}

		return 0;
	}

	void usage(ostream &out, string const &cmd) {
		out << "Usage: " << cmd << " [options]" << endl
		    << "Options:" << endl
		    << "  {-h|--help}" << endl
		    << "  {-i|--input} <input xml file>" << endl
		    << "  {-o|--output} <output device or file>" << endl
		    << "  {-V|--version}" << endl;
	}
}

int main(int argc, char **argv)
{
	int c;
	string input, output;
	char const *prog_name = basename(argv[0]);
	char const *short_opts = "hi:o:V";
	option const long_opts[] = {
		{ "help", no_argument, NULL, 'h'},
		{ "input", required_argument, NULL, 'i' },
		{ "output", required_argument, NULL, 'o'},
		{ "version", no_argument, NULL, 'V'},
		{ NULL, no_argument, NULL, 0 }
	};

	while ((c = getopt_long(argc, argv, short_opts, long_opts, NULL)) != -1) {
		switch(c) {
		case 'h':
			usage(cout, prog_name);
			return 0;

		case 'i':
			input = optarg;
			break;

		case 'o':
			output = optarg;
			break;

		case 'V':
			cout << THIN_PROVISIONING_TOOLS_VERSION << endl;
			return 0;

		default:
			usage(cerr, prog_name);
			return 1;
		}
	}

	if (argc != optind) {
		usage(cerr, prog_name);
		return 1;
	}

        if (input.empty()) {
		cerr << "No input file provided." << endl << endl;
		usage(cerr, prog_name);
		return 1;
	}

	if (output.empty()) {
		cerr << "No output file provided." << endl << endl;
		usage(cerr, prog_name);
		return 1;
	}

	return restore(input, output);
}

//----------------------------------------------------------------


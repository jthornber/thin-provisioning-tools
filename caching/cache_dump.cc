#include <fstream>
#include <getopt.h>
#include <libgen.h>
#include <iostream>

#include "version.h"
#include "caching/commands.h"
#include "caching/mapping_array.h"
#include "caching/metadata.h"
#include "caching/metadata_dump.h"
#include "caching/xml_format.h"
#include "persistent-data/file_utils.h"

using namespace std;
using namespace caching;

//----------------------------------------------------------------

namespace {
	struct flags {
		flags()
			: repair_(false) {
		}

		bool repair_;
	};

	//--------------------------------

	string const STDOUT_PATH("-");

	bool want_stdout(string const &output) {
		return output == STDOUT_PATH;
	}

	int dump(string const &dev, string const &output, flags const &fs) {
		try {
			block_manager<>::ptr bm = open_bm(dev, block_manager<>::READ_ONLY);
			metadata::ptr md(new metadata(bm, metadata::OPEN));

			if (want_stdout(output)) {
				emitter::ptr e = create_xml_emitter(cout);
				metadata_dump(md, e, fs.repair_);
			} else {
				ofstream out(output.c_str());
				emitter::ptr e = create_xml_emitter(out);
				metadata_dump(md, e, fs.repair_);
			}

		} catch (std::exception &e) {
			cerr << e.what() << endl;
			return 1;
		}

		return 0;
	}

	void usage(ostream &out, string const &cmd) {
		out << "Usage: " << cmd << " [options] {device|file}" << endl
		    << "Options:" << endl
		    << "  {-h|--help}" << endl
		    << "  {-o <xml file>}" << endl
		    << "  {-V|--version}" << endl
		    << "  {--repair}" << endl;
	}
}

//----------------------------------------------------------------

int cache_dump_main(int argc, char **argv)
{
	int c;
	flags fs;
	string output("-");
	char const shortopts[] = "ho:V";

	option const longopts[] = {
		{ "help", no_argument, NULL, 'h' },
		{ "output", required_argument, NULL, 'o' },
		{ "version", no_argument, NULL, 'V' },
		{ "repair", no_argument, NULL, 1 },
		{ NULL, no_argument, NULL, 0 }
	};

	while ((c = getopt_long(argc, argv, shortopts, longopts, NULL)) != -1) {
		switch(c) {
		case 1:
			fs.repair_ = true;
			break;

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

	return dump(argv[optind], output, fs);
}

base::command caching::cache_dump_cmd("cache_dump", cache_dump_main);

//----------------------------------------------------------------

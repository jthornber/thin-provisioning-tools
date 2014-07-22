#include "version.h"

#include "caching/metadata.h"
#include "caching/restore_emitter.h"
#include "caching/xml_format.h"
#include "persistent-data/file_utils.h"

#include <boost/lexical_cast.hpp>
#include <fstream>
#include <getopt.h>
#include <iostream>
#include <libgen.h>
#include <string>

using namespace boost;
using namespace caching;
using namespace persistent_data;
using namespace std;

//----------------------------------------------------------------

namespace {
	struct flags {
		flags()
			: metadata_version(1),
			  override_metadata_version(false),
			  clean_shutdown(true) {
		}

		optional<string> input;
		optional<string> output;

		uint32_t metadata_version;
		bool override_metadata_version;
		bool clean_shutdown;
	};

	int restore(flags const &fs) {
		try {
			block_manager<>::ptr bm = open_bm(*fs.output, block_manager<>::READ_WRITE);
			metadata::ptr md(new metadata(bm, metadata::CREATE));
			emitter::ptr restorer = create_restore_emitter(md, fs.clean_shutdown);

			if (fs.override_metadata_version) {
				cerr << "overriding" << endl;
				md->sb_.version = fs.metadata_version;
			}

			check_file_exists(*fs.input);
			ifstream in(fs.input->c_str(), ifstream::in);
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
		    << "  {-V|--version}" << endl
		    << endl
		    << "  {--debug-override-metadata-version} <integer>" << endl
		    << "  {--omit-clean-shutdown}" << endl;

	}
}

int main(int argc, char **argv)
{
	int c;
	flags fs;
	char const *prog_name = basename(argv[0]);
	char const *short_opts = "hi:o:V";
	option const long_opts[] = {
		{ "debug-override-metadata-version", required_argument, NULL, 0 },
		{ "omit-clean-shutdown", no_argument, NULL, 1 },
		{ "help", no_argument, NULL, 'h'},
		{ "input", required_argument, NULL, 'i' },
		{ "output", required_argument, NULL, 'o'},
		{ "version", no_argument, NULL, 'V'},
		{ NULL, no_argument, NULL, 0 }
	};

	while ((c = getopt_long(argc, argv, short_opts, long_opts, NULL)) != -1) {
		switch(c) {
		case 0:
			fs.metadata_version = lexical_cast<uint32_t>(optarg);
			fs.override_metadata_version = true;
			break;

		case 1:
			fs.clean_shutdown = false;
			break;

		case 'h':
			usage(cout, prog_name);
			return 0;

		case 'i':
			fs.input = optional<string>(string(optarg));
			break;

		case 'o':
			fs.output = optional<string>(string(optarg));
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

        if (!fs.input) {
		cerr << "No input file provided." << endl << endl;
		usage(cerr, prog_name);
		return 1;
	}

	if (!fs.output) {
		cerr << "No output file provided." << endl << endl;
		usage(cerr, prog_name);
		return 1;
	}

	return restore(fs);
}

//----------------------------------------------------------------

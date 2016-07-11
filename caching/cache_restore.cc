#include "version.h"

#include "base/output_file_requirements.h"
#include "caching/commands.h"
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
	size_t get_file_length(string const &file) {
		struct stat info;
		int r;

		r = ::stat(file.c_str(), &info);
		if (r)
			throw runtime_error("Couldn't stat backup path");

		return info.st_size;
	}

	auto_ptr<progress_monitor> create_monitor(bool quiet) {
		if (!quiet && isatty(fileno(stdout)))
			return create_progress_bar("Restoring");
		else
			return create_quiet_progress_monitor();
	}

	struct flags {
		flags()
			: metadata_version(1),
			  override_metadata_version(false),
			  clean_shutdown(true),
			  quiet(false) {
		}

		optional<string> input;
		optional<string> output;

		uint32_t metadata_version;
		bool override_metadata_version;
		bool clean_shutdown;
		bool quiet;
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

			auto_ptr<progress_monitor> monitor = create_monitor(fs.quiet);
			parse_xml(in, restorer, get_file_length(*fs.input), *monitor);

		} catch (std::exception &e) {
			cerr << e.what() << endl;
			return 1;
		}

		return 0;
	}
}

//----------------------------------------------------------------

cache_restore_cmd::cache_restore_cmd()
	: command("cache_restore")
{
}

void
cache_restore_cmd::usage(std::ostream &out) const
{
	out << "Usage: " << get_name() << " [options]" << endl
	    << "Options:" << endl
	    << "  {-h|--help}" << endl
	    << "  {-i|--input} <input xml file>" << endl
	    << "  {-o|--output} <output device or file>" << endl
	    << "  {-q|--quiet}" << endl
	    << "  {-V|--version}" << endl
	    << endl
	    << "  {--debug-override-metadata-version} <integer>" << endl
	    << "  {--omit-clean-shutdown}" << endl;
}

int
cache_restore_cmd::run(int argc, char **argv)
{
	int c;
	flags fs;
	char const *short_opts = "hi:o:qV";
	option const long_opts[] = {
		{ "debug-override-metadata-version", required_argument, NULL, 0 },
		{ "omit-clean-shutdown", no_argument, NULL, 1 },
		{ "help", no_argument, NULL, 'h'},
		{ "input", required_argument, NULL, 'i' },
		{ "output", required_argument, NULL, 'o'},
		{ "quiet", no_argument, NULL, 'q'},
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
			usage(cout);
			return 0;

		case 'i':
			fs.input = optional<string>(string(optarg));
			break;

		case 'o':
			fs.output = optional<string>(string(optarg));
			break;

		case 'q':
			fs.quiet = true;
			break;

		case 'V':
			cout << THIN_PROVISIONING_TOOLS_VERSION << endl;
			return 0;

		default:
			usage(cerr);
			return 1;
		}
	}

	if (argc != optind) {
		usage(cerr);
		return 1;
	}

        if (!fs.input) {
		cerr << "No input file provided." << endl << endl;
		usage(cerr);
		return 1;
	}

	if (fs.output)
		check_output_file_requirements(*fs.output);

	else {
		cerr << "No output file provided." << endl << endl;
		usage(cerr);
		return 1;
	}

	return restore(fs);
}

//----------------------------------------------------------------

#include <iostream>
#include <getopt.h>
#include <libgen.h>

#include "human_readable_format.h"
#include "metadata_dumper.h"
#include "metadata.h"
#include "restore_emitter.h"
#include "version.h"

using namespace persistent_data;
using namespace std;
using namespace thin_provisioning;

namespace {
	int repair(string const &old_path, string const &new_path) {
		try {
			// block size gets updated by the restorer
			metadata::ptr new_md(new metadata(new_path, metadata::CREATE, 128, 0));
			emitter::ptr e = create_restore_emitter(new_md);

			metadata::ptr old_md(new metadata(old_path, metadata::OPEN));
			metadata_dump(old_md, e, true);

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
		    << "  {-i|--input} <input metadata (binary format)>" << endl
		    << "  {-o|--output} <output metadata (binary format)>" << endl
		    << "  {-V|--version}" << endl;
	}
}

int main(int argc, char **argv)
{
	int c;
	boost::optional<string> input_path, output_path;
	const char shortopts[] = "hi:o:V";

	const struct option longopts[] = {
		{ "help", no_argument, NULL, 'h'},
		{ "input", required_argument, NULL, 'i'},
		{ "output", required_argument, NULL, 'o'},
		{ "version", no_argument, NULL, 'V'},
		{ NULL, no_argument, NULL, 0 }
	};

	while ((c = getopt_long(argc, argv, shortopts, longopts, NULL)) != -1) {
		switch(c) {
		case 'h':
			usage(cout, basename(argv[0]));
			return 0;

		case 'i':
			input_path = optarg;
			break;

		case 'o':
			output_path = optarg;
			break;

		case 'V':
			cout << THIN_PROVISIONING_TOOLS_VERSION << endl;
			return 0;

		default:
			usage(cerr, basename(argv[0]));
			return 1;
		}
	}

	if (!input_path) {
		cerr << "no input file provided" << endl;
		usage(cerr, basename(argv[0]));
		return 1;
	}

	if (!output_path) {
		cerr << "no output file provided" << endl;
		usage(cerr, basename(argv[0]));
		return 1;
	}

	return repair(*input_path, *output_path);
}

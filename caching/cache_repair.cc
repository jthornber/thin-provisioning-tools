#include <iostream>
#include <getopt.h>
#include <libgen.h>

#include "base/file_utils.h"
#include "base/output_file_requirements.h"
#include "caching/commands.h"
#include "caching/metadata.h"
#include "caching/metadata_dump.h"
#include "caching/restore_emitter.h"
#include "persistent-data/file_utils.h"
#include "version.h"

using namespace persistent_data;
using namespace std;
using namespace caching;

//----------------------------------------------------------------

namespace {
	metadata::ptr open_metadata_for_read(string const &path) {
		block_manager::ptr bm = open_bm(path, block_manager::READ_ONLY);
		return metadata::ptr(new metadata(bm));
	}

	emitter::ptr output_emitter(string const &path) {
		block_manager::ptr bm = open_bm(path, block_manager::READ_WRITE);
		metadata::ptr md(new metadata(bm, metadata::CREATE));
		return create_restore_emitter(md, true);
	}

	int repair(string const &old_path, string const &new_path) {
		bool metadata_touched = false;
		try {
			file_utils::check_file_exists(old_path, false);
			metadata_touched = true;
			metadata_dump(open_metadata_for_read(old_path),
				      output_emitter(new_path),
				      true);

		} catch (std::exception &e) {
			if (metadata_touched)
				file_utils::zero_superblock(new_path);
			cerr << e.what() << endl;
			return 1;
		}

		return 0;
	}
}

//----------------------------------------------------------------

cache_repair_cmd::cache_repair_cmd()
	: command("cache_repair")
{
}

void
cache_repair_cmd::usage(std::ostream &out) const
{
	out << "Usage: " << get_name() << " [options] {device|file}" << endl
	    << "Options:" << endl
	    << "  {-h|--help}" << endl
	    << "  {-i|--input} <input metadata (binary format)>" << endl
	    << "  {-o|--output} <output metadata (binary format)>" << endl
	    << "  {-V|--version}" << endl;
}

int
cache_repair_cmd::run(int argc, char **argv)
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
			usage(cout);
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
			usage(cerr);
			return 1;
		}
	}

	if (!input_path) {
		cerr << "no input file provided" << endl;
		usage(cerr);
		return 1;
	}

	if (output_path)
		check_output_file_requirements(*output_path);

	else {
		cerr << "No output file provided." << endl;
		usage(cerr);
		return 1;
	}

	return repair(*input_path, *output_path);
}

//----------------------------------------------------------------

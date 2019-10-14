// Copyright (C) 2011 Red Hat, Inc. All rights reserved.
//
// This file is part of the thin-provisioning-tools source.
//
// thin-provisioning-tools is free software: you can redistribute it
// and/or modify it under the terms of the GNU General Public License
// as published by the Free Software Foundation, either version 3 of
// the License, or (at your option) any later version.
//
// thin-provisioning-tools is distributed in the hope that it will be
// useful, but WITHOUT ANY WARRANTY; without even the implied warranty
// of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License along
// with thin-provisioning-tools.  If not, see
// <http://www.gnu.org/licenses/>.

#include "base/file_utils.h"
#include "base/output_file_requirements.h"
#include "persistent-data/file_utils.h"
#include "thin-provisioning/commands.h"
#include "thin-provisioning/emitter.h"
#include "thin-provisioning/human_readable_format.h"
#include "thin-provisioning/metadata.h"
#include "thin-provisioning/restore_emitter.h"
#include "thin-provisioning/xml_format.h"
#include "version.h"

#include <fstream>
#include <getopt.h>
#include <iostream>
#include <libgen.h>
#include <linux/fs.h>
#include <sys/ioctl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

using namespace persistent_data;
using namespace std;
using namespace thin_provisioning;

//----------------------------------------------------------------

namespace {
	int restore(string const &backup_file, string const &dev, bool quiet, restore_options const &opts) {
		bool metadata_touched = false;
		try {
			// The block size gets updated by the restorer.
			block_manager<>::ptr bm(open_bm(dev, block_manager<>::READ_WRITE));
			file_utils::check_file_exists(backup_file);
			metadata_touched = true;
			metadata::ptr md(new metadata(bm, metadata::CREATE, 128, 0));
			emitter::ptr restorer = create_restore_emitter(md, opts);

			parse_xml(backup_file, restorer, quiet);

		} catch (std::exception &e) {
			if (metadata_touched)
				file_utils::zero_superblock(dev);
			cerr << e.what() << endl;
			return 1;
		}

		return 0;
	}
}

//----------------------------------------------------------------

thin_restore_cmd::thin_restore_cmd()
	: command("thin_restore")
{
}

void
thin_restore_cmd::usage(std::ostream &out) const
{
	out << "Usage: " << get_name() << " [options]" << endl
	    << "Options:" << endl
	    << "  {-h|--help}" << endl
	    << "  {-i|--input} <input xml file>" << endl
	    << "  {-o|--output} <output device or file>" << endl
	    << "  {--transaction-id} <natural>" << endl
	    << "  {--data-block-size} <natural>" << endl
	    << "  {--nr-data-blocks} <natural>" << endl
	    << "  {-q|--quiet}" << endl
	    << "  {-V|--version}" << endl;
}

int
thin_restore_cmd::run(int argc, char **argv)
{
	int c;
	const char *shortopts = "hi:o:qV";
	string input, output;
	bool quiet = false;
	restore_options opts;

	const struct option longopts[] = {
		{ "help", no_argument, NULL, 'h'},
		{ "input", required_argument, NULL, 'i' },
		{ "output", required_argument, NULL, 'o'},
		{ "transaction-id", required_argument, NULL, 1},
		{ "data-block-size", required_argument, NULL, 2},
		{ "nr-data-blocks", required_argument, NULL, 3},
		{ "quiet", no_argument, NULL, 'q'},
		{ "version", no_argument, NULL, 'V'},
		{ NULL, no_argument, NULL, 0 }
	};

	while ((c = getopt_long(argc, argv, shortopts, longopts, NULL)) != -1) {
		switch(c) {
		case 'h':
			usage(cout);
			return 0;

		case 'i':
			input = optarg;
			break;

		case 'o':
			output = optarg;
			break;

		case 1:
			opts.transaction_id_ = parse_uint64(optarg, "transaction id");
			break;

		case 2:
			opts.data_block_size_ = static_cast<uint32_t>(parse_uint64(optarg, "data block size"));
			break;

		case 3:
			opts.nr_data_blocks_ = parse_uint64(optarg, "nr data blocks");
			break;

		case 'q':
			quiet = true;
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

        if (input.empty()) {
		cerr << "No input file provided." << endl << endl;
		usage(cerr);
		return 1;
	}

	if (output.empty()) {
		cerr << "No output file provided." << endl << endl;
		usage(cerr);
		return 1;
	} else
		check_output_file_requirements(output);

	return restore(input, output, quiet, opts);
}

//----------------------------------------------------------------

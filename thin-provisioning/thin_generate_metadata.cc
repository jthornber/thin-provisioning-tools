// Copyright (C) 2016 Red Hat, Inc. All rights reserved.
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

#include "base/output_file_requirements.h"
#include "thin-provisioning/commands.h"
#include "thin-provisioning/metadata.h"
#include "version.h"

#include <boost/optional.hpp>
#include <getopt.h>
#include <unistd.h>

using namespace boost;
using namespace persistent_data;
using namespace std;
using namespace thin_provisioning;

//----------------------------------------------------------------

namespace {
	struct flags {
		flags()
			: data_block_size(128),
			  nr_data_blocks(10240)
		{
		}

		block_address data_block_size;
		block_address nr_data_blocks;
		optional<string> output;
	};
}

//----------------------------------------------------------------

thin_generate_metadata_cmd::thin_generate_metadata_cmd()
	: command("thin_generate_metadata")
{
}

void
thin_generate_metadata_cmd::usage(std::ostream &out) const
{
	out << "Usage: " << get_name() << " [options]\n"
	    << "Options:\n"
	    << "  {-h|--help}\n"
	    << "  --data-block-size <block size>\n"
	    << "  --nr-data-blocks <nr>\n"
	    << "  {-o|--output} <output device or file>\n"
	    << "  {-V|--version}" << endl;
}

int
thin_generate_metadata_cmd::run(int argc, char **argv)
{
	int c;
	struct flags fs;
	const char *shortopts = "hi:o:qV";
	const struct option longopts[] = {
		{ "help", no_argument, NULL, 'h' },
		{ "output", required_argument, NULL, 'o' },
		{ "data-block-size", required_argument, NULL, 101 },
		{ "nr-data-blocks", required_argument, NULL, 102 },
		{ "version", no_argument, NULL, 'V' },
		{ NULL, no_argument, NULL, 0 }
	};

	while ((c = getopt_long(argc, argv, shortopts, longopts, NULL)) != -1) {
		switch(c) {
		case 'h':
			usage(cout);
			return 0;

		case 'o':
			fs.output = optarg;
			break;

		case 101:
			fs.data_block_size = parse_uint64(optarg, "data block size");
			break;

		case 102:
			fs.nr_data_blocks = parse_uint64(optarg, "nr data blocks");
			break;

		case 'V':
			cout << THIN_PROVISIONING_TOOLS_VERSION << endl;
			return 0;

		default:
			usage(cerr);
			return 1;
		}
	}

	if (!fs.output) {
		cerr << "No output file provided.\n\n";
		usage(cerr);
		return 1;
	} else
		check_output_file_requirements(*fs.output);

	return 0;
}

//----------------------------------------------------------------

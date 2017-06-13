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
#include "persistent-data/file_utils.h"
#include "thin-provisioning/commands.h"
#include "thin-provisioning/emitter.h"
#include "thin-provisioning/human_readable_format.h"
#include "thin-provisioning/metadata.h"
#include "thin-provisioning/restore_emitter.h"
#include "thin-provisioning/xml_format.h"
#include "version.h"

#include <boost/optional.hpp>
#include <fstream>
#include <getopt.h>
#include <iostream>
#include <libgen.h>
#include <linux/fs.h>
#include <sys/ioctl.h>
#include <sys/stat.h>
#include <sys/types.h>
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
			  nr_data_blocks(10240),
			  nr_thins(1),
			  blocks_per_thin(1024),
			  run_lengths(1024) {
		}

		block_address data_block_size;
		block_address nr_data_blocks;
		unsigned nr_thins;
		block_address blocks_per_thin;
		block_address run_lengths;
		optional<string> output;
	};

	// This is how we stir in some entropy.  It mixes up the data
	// device.
	class shuffler {
	public:
		shuffler(block_address nr_blocks, unsigned run_lengths)
			: nr_blocks_(nr_blocks / run_lengths),
			  run_lengths_(run_lengths) {
		}

		block_address map(block_address b) const {
			return reverse(b / run_lengths_) + (b % run_lengths_);
		}

	private:
		block_address reverse(block_address b) const {
			return nr_blocks_ - b - 1ull;
		}

		block_address nr_blocks_;
		block_address run_lengths_;
	};

	void generate_device(emitter::ptr e, shuffler const &s, uint32_t dev_id,
			     block_address nr_blocks, block_address base) {

		e->begin_device(dev_id, nr_blocks, 0, 0, 0);
		for (unsigned b = 0; b < nr_blocks; b++)
			e->single_map(b, s.map(base + b), 0);
		e->end_device();
	}

	void generate_metadata(flags const &fs, emitter::ptr e) {
		e->begin_superblock("fake metadata", 0, 0, optional<uint32_t>(), optional<uint32_t>(),
				    fs.data_block_size, fs.nr_data_blocks, optional<uint64_t>());

		shuffler s(fs.nr_data_blocks, fs.run_lengths);
		for (unsigned i = 0; i < fs.nr_thins; i++)
			generate_device(e, s, i, fs.blocks_per_thin, i * fs.blocks_per_thin);

		e->end_superblock();
	}

	int create_metadata(flags const &fs) {
		try {
			// The block size gets updated by the restorer.
			block_manager<>::ptr bm(open_bm(*fs.output, block_manager<>::READ_WRITE));
			metadata::ptr md(new metadata(bm, metadata::CREATE, 128, 0));
			emitter::ptr restorer = create_restore_emitter(md);

			generate_metadata(fs, restorer);

		} catch (std::exception &e) {
			cerr << e.what() << endl;
			return 1;
		}

		return 0;
	}
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
	    << "  --nr-thins <count>\n"
	    << "  --blocks-per-thin <count>\n"
	    << "  --run-lengths <count>\n"
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
		{ "help", no_argument, NULL, 'h'},
		{ "output", required_argument, NULL, 'o'},
		{ "data-block-size", required_argument, NULL, 1},
		{ "nr-data-blocks", required_argument, NULL, 2},
		{ "nr-thins", required_argument, NULL, 3},
		{ "blocks-per-thin", required_argument, NULL, 4},
		{ "run-lengths", required_argument, NULL, 5},
		{ "version", no_argument, NULL, 'V'},
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

		case 1:
			fs.data_block_size = parse_uint64(optarg, "data block size");
			break;

		case 2:
			fs.nr_data_blocks = parse_uint64(optarg, "nr data blocks");
			break;

		case 3:
			fs.nr_thins = parse_uint64(optarg, "nr thins");
			break;

		case 4:
			fs.blocks_per_thin = parse_uint64(optarg, "blocks per thin");
			break;

		case 5:
			fs.run_lengths = parse_uint64(optarg, "run lengths");
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

	return create_metadata(fs);
}

//----------------------------------------------------------------

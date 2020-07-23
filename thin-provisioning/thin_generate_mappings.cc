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

#include "base/io_generator.h"
#include "base/output_file_requirements.h"
#include "persistent-data/file_utils.h"
#include "thin-provisioning/commands.h"
#include "thin-provisioning/thin_pool.h"
#include "version.h"

#include <boost/optional.hpp>
#include <getopt.h>
#include <unistd.h>

using namespace boost;
using namespace thin_provisioning;

//----------------------------------------------------------------

namespace {
	struct flags {
		flags()
			: pattern("write"),
			  offset(0)
		{
		}

		bool check_conformance();

		boost::optional<string> output;
		base::io_pattern pattern;
		boost::optional<unsigned> dev_id;
		boost::optional<base::sector_t> block_size;
		base::sector_t offset;
		boost::optional<base::sector_t> size;
		boost::optional<base::sector_t> io_size;
		boost::optional<unsigned> nr_seq_blocks;
	};

	bool flags::check_conformance() {
		if (!output) {
			cerr << "No output file provided." << endl;
			return false;
		}

		if (!dev_id) {
			cerr << "No device id provided." << endl;
			return false;
		}

		if (!size) {
			cerr << "No device size specified" << endl;
			return false;
		}

		if (nr_seq_blocks) {
			if (!pattern.is_random()) {
				cerr << "Cannot specify the sequence size"
					" while doing non-random IO" << endl;
				return false;
			}
		}

		check_output_file_requirements(*output);

		return true;
	}

	//--------------------------------

	thin_pool::ptr open_pool(flags const &fs) {
		block_manager::ptr bm = open_bm(*fs.output, block_manager::READ_WRITE);
		return thin_pool::ptr(new thin_pool(bm));
	}

	int generate_mappings(flags const &fs) {
		thin_pool::ptr pool = open_pool(fs);

		thin::ptr td = pool->open_thin(*fs.dev_id);

		io_generator_options opts;
		opts.pattern_ = fs.pattern;
		opts.block_size_ = !fs.block_size ?
				   pool->get_data_block_size() :
				   *fs.block_size;
		opts.offset_ = fs.offset;
		opts.size_ = *fs.size;
		opts.io_size_ = !fs.io_size ? *fs.size : *fs.io_size;
		opts.nr_seq_blocks_ = !fs.nr_seq_blocks ? 1 : *fs.nr_seq_blocks;
		io_generator::ptr gen = create_io_generator(opts);

		base::io io;
		while (gen->next(io)) {
			// TODO: support io.size_
			switch (io.op_) {
			case base::REQ_OP_READ:
				process_read(td, pool, io.sector_);
				break;
			case base::REQ_OP_WRITE:
				process_write(td, pool, io.sector_);
				break;
			case base::REQ_OP_DISCARD:
				process_discard(td, pool, io.sector_);
				break;
			}
		}

		pool->commit();

		return 0;
	}
}

//----------------------------------------------------------------

thin_generate_mappings_cmd::thin_generate_mappings_cmd()
	: command("thin_generate_mappings")
{
}

void
thin_generate_mappings_cmd::usage(std::ostream &out) const
{
	out << "Usage: " << get_name() << " [options]\n"
	    << "Options:\n"
	    << "  {-h|--help}\n"
	    << "  {-o|--output} <output device or file>\n"
	    << "  {--dev-id} <dev-id>\n"
	    << "  {--offset} <offset in sectors>\n"
	    << "  {--io-size} <io-size in sectors>\n"
	    << "  {--rw write|trim|randwrite|randtrim|randtw}\n"
	    << "  {--size} <size in sectors>\n"
	    << "  {--seq-nr} <max nr. of sequential ios>\n"
	    << "  {-V|--version}" << endl;
}

int
thin_generate_mappings_cmd::run(int argc, char **argv)
{
	int c;
	struct flags fs;
	const char *shortopts = "ho:V";
	const struct option longopts[] = {
		{ "help", no_argument, NULL, 'h' },
		{ "output", required_argument, NULL, 'o' },
		{ "dev-id", required_argument, NULL, 1 },
		{ "rw", required_argument, NULL, 2 },
		{ "offset", required_argument, NULL, 3 },
		{ "size", required_argument, NULL, 4 },
		{ "io-size", required_argument, NULL, 5 },
		{ "seq-nr", required_argument, NULL, 6 },
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

		case 1:
			fs.dev_id = parse_uint64(optarg, "dev_id");
			break;

		case 2:
			fs.pattern.parse(optarg);
			break;

		case 3:
			fs.offset = parse_uint64(optarg, "offset");
			break;

		case 4:
			fs.size = parse_uint64(optarg, "size");
			break;

		case 5:
			fs.io_size = parse_uint64(optarg, "io_size");
			break;

		case 6:
			fs.nr_seq_blocks = parse_uint64(optarg, "seq_nr");
			break;

		case 'V':
			cout << THIN_PROVISIONING_TOOLS_VERSION << endl;
			return 0;

		default:
			usage(cerr);
			return 1;
		}
	}

	if (!fs.check_conformance()) {
		usage(cerr);
		return 1;
	}

	return generate_mappings(fs);
}

//----------------------------------------------------------------

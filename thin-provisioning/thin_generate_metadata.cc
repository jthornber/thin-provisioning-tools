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
		enum metadata_operations {
			METADATA_OP_NONE,
			METADATA_OP_FORMAT,
			METADATA_OP_OPEN,
			METADATA_OP_CREATE_THIN,
			METADATA_OP_CREATE_SNAP,
			METADATA_OP_DELETE_DEV,
			METADATA_OP_SET_TRANSACTION_ID,
			METADATA_OP_RESERVE_METADATA_SNAP,
			METADATA_OP_RELEASE_METADATA_SNAP,
			METADATA_OP_LAST
		};

		flags()
			: op(METADATA_OP_NONE),
			  data_block_size(128),
			  nr_data_blocks(10240)
		{
		}

		bool check_conformance();

		metadata_operations op;
		sector_t data_block_size;
		block_address nr_data_blocks;
		optional<thin_dev_t> dev_id;
		optional<thin_dev_t> origin;
		optional<uint64_t> trans_id;
		optional<string> output;
	};

	// FIXME: modulize the conditions
	bool flags::check_conformance() {
		if (op == METADATA_OP_NONE || op >= METADATA_OP_LAST) {
			cerr << "Invalid operation." << endl;
			return false;
		}

		if (!output) {
			cerr << "No output file provided." << endl;
			return false;
		} else
			check_output_file_requirements(*output);

		if (op == METADATA_OP_CREATE_THIN && !dev_id) {
			cerr << "no device id provided." << endl;
			return false;
		}

		if (op == METADATA_OP_CREATE_SNAP && (!dev_id || !origin)) {
			cerr << "no device id provided." << endl;
			return false;
		}

		if (op == METADATA_OP_DELETE_DEV && !dev_id) {
			cerr << "no device id provided." << endl;
			return false;
		}

		if (op == METADATA_OP_SET_TRANSACTION_ID && !trans_id) {
			cerr << "no transaction id provided." << endl;
			return false;
		}

		return true;
	}

	//--------------------------------

	thin_pool::ptr open_or_create_pool(flags const &fs) {
		block_manager::ptr bm = open_bm(*fs.output, block_manager::READ_WRITE);

		if (fs.op == flags::METADATA_OP_FORMAT)
			return thin_pool::ptr(new thin_pool(bm, fs.data_block_size, fs.nr_data_blocks));
		else
			return thin_pool::ptr(new thin_pool(bm));
	}

	int generate_metadata(flags const &fs) {
		thin_pool::ptr pool = open_or_create_pool(fs);

		switch (fs.op) {
		case flags::METADATA_OP_CREATE_THIN:
			pool->create_thin(*fs.dev_id);
			break;
		case flags::METADATA_OP_CREATE_SNAP:
			pool->create_snap(*fs.dev_id, *fs.origin);
			break;
		case flags::METADATA_OP_DELETE_DEV:
			pool->del(*fs.dev_id);
			break;
		case flags::METADATA_OP_SET_TRANSACTION_ID:
			pool->set_transaction_id(*fs.trans_id);
			break;
		case flags::METADATA_OP_RESERVE_METADATA_SNAP:
			pool->reserve_metadata_snap();
			break;
		case flags::METADATA_OP_RELEASE_METADATA_SNAP:
			pool->release_metadata_snap();
			break;
		default:
			break;
		}

		pool->commit();

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
	    << "  {--format}\n"
	    << "  {--create-thin} <dev-id>\n"
	    << "  {--create-snap} <dev-id>\n"
	    << "  {--delete} <dev-id>\n"
	    << "  {--reserve-metadata-snap}\n"
	    << "  {--release-metadata-snap}\n"
	    << "  {--set-transaction-id} <tid>\n"
	    << "  {--data-block-size} <block size>\n"
	    << "  {--nr-data-blocks} <nr>\n"
	    << "  {--origin} <origin-id>\n"
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
		{ "format", no_argument, NULL, 1 },
		{ "open", no_argument, NULL, 2 },
		{ "create-thin", required_argument, NULL, 3 },
		{ "create-snap", required_argument, NULL, 4 },
		{ "delete", required_argument, NULL, 5 },
		{ "set-transaction-id", required_argument, NULL, 6 },
		{ "reserve-metadata-snap", no_argument, NULL, 7 },
		{ "release-metadata-snap", no_argument, NULL, 8 },
		{ "data-block-size", required_argument, NULL, 101 },
		{ "nr-data-blocks", required_argument, NULL, 102 },
		{ "origin", required_argument, NULL, 401 },
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
			fs.op = flags::METADATA_OP_FORMAT;
			break;

		case 2:
			fs.op = flags::METADATA_OP_OPEN;
			break;

		case 3:
			fs.op = flags::METADATA_OP_CREATE_THIN;
			fs.dev_id = parse_uint64(optarg, "device id");
			break;

		case 4:
			fs.op = flags::METADATA_OP_CREATE_SNAP;
			fs.dev_id = parse_uint64(optarg, "device id");
			break;

		case 5:
			fs.op = flags::METADATA_OP_DELETE_DEV;
			fs.dev_id = parse_uint64(optarg, "device id");
			break;

		case 6:
			fs.op = flags::METADATA_OP_SET_TRANSACTION_ID;
			fs.trans_id = parse_uint64(optarg, "transaction id");
			break;

		case 7:
			fs.op = flags::METADATA_OP_RESERVE_METADATA_SNAP;
			break;

		case 8:
			fs.op = flags::METADATA_OP_RELEASE_METADATA_SNAP;
			break;

		case 101:
			fs.data_block_size = parse_uint64(optarg, "data block size");
			break;

		case 102:
			fs.nr_data_blocks = parse_uint64(optarg, "nr data blocks");
			break;

		case 401:
			fs.origin = parse_uint64(optarg, "origin");
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

	return generate_metadata(fs);
}

//----------------------------------------------------------------

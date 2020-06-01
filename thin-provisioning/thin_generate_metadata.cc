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
		enum metadata_operations {
			METADATA_OP_NONE,
			METADATA_OP_FORMAT,
			METADATA_OP_OPEN,
			METADATA_OP_CREATE_THIN,
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
		optional<uint64_t> dev_id;
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

		return true;
	}

	//--------------------------------

	single_mapping_tree::ptr new_mapping_tree(metadata::ptr md) {
		return single_mapping_tree::ptr(
			new single_mapping_tree(*md->tm_,
						mapping_tree_detail::block_time_ref_counter(md->data_sm_)));
	}

	bool is_device_exists(metadata::ptr md, uint64_t dev_id) {
		uint64_t key[1] = {dev_id};

		device_tree::maybe_value v1 = md->details_->lookup(key);
		if (v1)
			return true;

		dev_tree::maybe_value v2 = md->mappings_top_level_->lookup(key);
		if (v2)
			return true;

		return false;
	}

	//--------------------------------

	metadata::ptr format_metadata(block_manager::ptr bm,
				      sector_t data_block_size,
				      block_address nr_data_blocks) {
		metadata::ptr md(new metadata(bm,
					      metadata::CREATE,
					      data_block_size,
					      nr_data_blocks));
		md->commit();
		return md;
	}

	metadata::ptr open_metadata(block_manager::ptr bm) {
		metadata::ptr md(new metadata(bm, true));
		return md;
	}

	void create_thin(metadata::ptr md, uint64_t dev_id) {
		uint64_t key[1] = {dev_id};

		if (is_device_exists(md, dev_id))
			throw runtime_error("device already exists");

		device_tree_detail::device_details details;
		details.transaction_id_ = md->sb_.trans_id_;
		details.creation_time_ = md->sb_.time_;
		details.snapshotted_time_ = details.creation_time_;
		md->details_->insert(key, details);

		single_mapping_tree::ptr subtree = new_mapping_tree(md);
		md->mappings_top_level_->insert(key, subtree->get_root());
		md->mappings_->set_root(md->mappings_top_level_->get_root()); // FIXME: ugly

		md->commit();
	}

	metadata::ptr open_or_format_metadata(block_manager::ptr bm, flags const &fs) {

		if (fs.op == flags::METADATA_OP_FORMAT)
			return format_metadata(bm, fs.data_block_size, fs.nr_data_blocks);
		else
			return open_metadata(bm);
	}

	int generate_metadata(flags const &fs) {
		block_manager::ptr bm = open_bm(*fs.output, block_manager::READ_WRITE);
		metadata::ptr md = open_or_format_metadata(bm, fs);

		switch (fs.op) {
		case flags::METADATA_OP_CREATE_THIN:
			create_thin(md, *fs.dev_id);
			break;
		default:
			break;
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
		{ "create-thin", no_argument, NULL, 3 },
		{ "data-block-size", required_argument, NULL, 101 },
		{ "nr-data-blocks", required_argument, NULL, 102 },
		{ "dev-id", required_argument, NULL, 301 },
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
			break;

		case 101:
			fs.data_block_size = parse_uint64(optarg, "data block size");
			break;

		case 102:
			fs.nr_data_blocks = parse_uint64(optarg, "nr data blocks");
			break;

		case 301:
			fs.dev_id = parse_uint64(optarg, "dev id");
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

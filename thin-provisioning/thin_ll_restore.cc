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

#include "base/xml_utils.h"
#include "metadata_dumper.h"
#include "metadata.h"
#include "persistent-data/file_utils.h"
#include "persistent-data/space-maps/disk_structures.h"
#include "restore_emitter.h"
#include "xml_format.h"
#include "thin-provisioning/commands.h"
#include "version.h"

#include <fstream>
#include <getopt.h>
#include <iostream>

using namespace persistent_data;
using namespace std;
using namespace thin_provisioning;
using namespace xml_utils;

//----------------------------------------------------------------

namespace {
	struct user_data {
		block_manager<>::ptr input_bm_;
		block_manager<>::ptr output_bm_;

		metadata::ptr md_;
		XML_Parser parser_;
		emitter::ptr emitter_;
	};

	void open_resources(user_data &ud, attributes const &attr) {
		boost::optional<uint64_t> val;

		// open the input metadata
		// Allow to read superblock at arbitrary location for low-level restore
		block_address sb_location = (val = get_opt_attr<uint64_t>(attr, "blocknr")) ?
					    *val : superblock_detail::SUPERBLOCK_LOCATION;
		ud.md_ = metadata::ptr(new metadata(ud.input_bm_, sb_location));

		// override superblock::device_details_root_
		if ((val = get_opt_attr<uint64_t>(attr, "device_details_root"))) {
			ud.md_->sb_.device_details_root_ = *val;
			ud.md_->details_ = device_tree::ptr(new device_tree(*ud.md_->tm_, *val,
							    device_tree_detail::device_details_traits::ref_counter()));
		}

		// open the output metadata
		metadata::ptr new_md(new metadata(ud.output_bm_, metadata::CREATE, 128, 0));

		ud.emitter_ = create_restore_emitter(new_md);
	}

	void parse_superblock(metadata::ptr md, emitter::ptr e, attributes const &attr) {
		sm_disk_detail::sm_root_disk const *d =
			reinterpret_cast<sm_disk_detail::sm_root_disk const *>(md->sb_.data_space_map_root_);
		sm_disk_detail::sm_root v;
		sm_disk_detail::sm_root_traits::unpack(*d, v);

		e->begin_superblock("", md->sb_.time_,
				    md->sb_.trans_id_,
				    md->sb_.flags_,
				    md->sb_.version_,
				    md->sb_.data_block_size_,
				    v.nr_blocks_,
				    boost::optional<block_address>());
	}

	void parse_device(metadata::ptr md, emitter::ptr e, attributes const &attr) {
		uint32_t dev_id = get_attr<uint32_t>(attr, "dev_id");
		device_tree_detail::device_details details;

		device_tree::ptr details_tree;
		boost::optional<uint64_t> details_root = get_opt_attr<uint64_t>(attr, "blocknr");
		if (details_root)
			details_tree = device_tree::ptr(new device_tree(*md->tm_, *details_root,
							device_tree_detail::device_details_traits::ref_counter()));
		else
			details_tree = md->details_;

		uint64_t key[1] = {dev_id};
		device_tree::maybe_value v;
		try {
			v = details_tree->lookup(key);
		} catch (std::exception &e) {
			cerr << "missing device " << dev_id << ": " << e.what() << endl;
		}
		if (v)
			details = *v;

		e->begin_device(dev_id,
				0,
				details.transaction_id_,
				details.creation_time_,
				details.snapshotted_time_);
	}

	void parse_node(metadata::ptr md, emitter::ptr e, attributes const &attr) {
		metadata_dump_subtree(md, e, true, get_attr<uint64_t>(attr, "blocknr"));
	}

	void start_tag(void *data, char const *el, char const **attr) {
		user_data *ud = static_cast<user_data *>(data);
		attributes a;

		build_attributes(a, attr);

		if (!strcmp(el, "superblock")) {
			open_resources(*ud, a);
			parse_superblock(ud->md_, ud->emitter_, a);

		} else if (!strcmp(el, "device"))
			parse_device(ud->md_, ud->emitter_, a);

		else if (!strcmp(el, "node"))
			parse_node(ud->md_, ud->emitter_, a);

		else
			throw runtime_error("unknown tag type");
	}

	void end_tag(void *data, const char *el) {
		user_data *ud = static_cast<user_data *>(data);

		if (!strcmp(el, "superblock")) {
			ud->emitter_->end_superblock();
			XML_StopParser(ud->parser_, XML_FALSE); // skip the rest elements
		}

		else if (!strcmp(el, "device"))
			ud->emitter_->end_device();

		else if (!strcmp(el, "node"))
			;

		else
			throw runtime_error("unknown tag type");
	}
}

//---------------------------------------------------------------------------

namespace {
	struct flags {
		flags() {
		}
	};

	int low_level_restore_(string const &src_metadata, string const &input,
			       string const &output, flags const &f) {
		user_data ud;
		ud.input_bm_ = open_bm(src_metadata, block_manager<>::READ_ONLY);
		ud.output_bm_ = open_bm(output, block_manager<>::READ_WRITE);

		xml_parser p;
		ud.parser_ = p.get_parser();

		XML_SetUserData(p.get_parser(), &ud);
		XML_SetElementHandler(p.get_parser(), start_tag, end_tag);

		bool quiet = true;
		p.parse(input, quiet);

		return 0;
	}

	int low_level_restore(string const &src_metadata, string const &input,
			      string const &output, flags const &f) {
		try {
			low_level_restore_(src_metadata, input, output, f);
		} catch (std::exception &e) {
			cerr << e.what() << endl;
			return 1;
		}
		return 0;
	}
}

//---------------------------------------------------------------------------

thin_ll_restore_cmd::thin_ll_restore_cmd()
	: command("thin_ll_restore")
{
}

void
thin_ll_restore_cmd::usage(ostream &out) const {
	out << "Usage: " << get_name() << " [options]" << endl
	    << "Options:" << endl
	    << "  {-h|--help}" << endl
	    << "  {-E|--source-metadata} <input device or file>" << endl
	    << "  {-i|--input} <input xml file>" << endl
	    << "  {-o|--output} <output device or file>" << endl
	    << "  {-V|--version}" << endl;
}

int
thin_ll_restore_cmd::run(int argc, char **argv) {
	string input;
	string output;
	string input_metadata;
	flags f;
	int c;

	const char shortopts[] = "hi:o:E:V";
	const struct option longopts[] = {
		{ "help", no_argument, NULL, 'h'},
		{ "input", required_argument, NULL, 'i'},
		{ "output", required_argument, NULL, 'o'},
		{ "source-metadata", required_argument, NULL, 'E'},
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

		case 'E':
			input_metadata = optarg;
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

	if (!input_metadata.length() || !input.length() || !output.length()) {
		cerr << "No input/output file provided." << endl;
		usage(cerr);
		return 1;
	}

	return low_level_restore(input_metadata, input, output, f);
}

//---------------------------------------------------------------------------

// Copyright (C) 2012 Red Hat, Inc. All rights reserved.
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

#include <boost/lexical_cast.hpp>
#include <getopt.h>
#include <iostream>
#include <string>

#include "base/math_utils.h"
#include "dbg-lib/array_block_dumper.h"
#include "dbg-lib/btree_node_dumper.h"
#include "dbg-lib/index_block_dumper.h"
#include "dbg-lib/command_interpreter.h"
#include "dbg-lib/commands.h"
#include "dbg-lib/output_formatter.h"
#include "dbg-lib/sm_show_traits.h"
#include "persistent-data/file_utils.h"
#include "persistent-data/space-maps/disk_structures.h"
#include "thin-provisioning/commands.h"
#include "thin-provisioning/metadata.h"
#include "thin-provisioning/superblock.h"
#include "version.h"

using namespace dbg;
using namespace persistent_data;
using namespace std;
using namespace thin_provisioning;

namespace {
	class help : public dbg::command {
		virtual void exec(strings const &args, ostream &out) {
			out << "Commands:" << endl
			    << "  superblock [block#]" << endl
			    << "  m1_node <block# of top-level mapping tree node>" << endl
			    << "  m2_node <block# of bottom-level mapping tree node>" << endl
			    << "  detail_node <block# of device details tree node>" << endl
			    << "  index_block <block# of metadata space map root>" << endl
			    << "  index_node <block# of data space map node>" << endl
			    << "  exit" << endl;
		}
	};

	class show_superblock : public dbg::command {
	public:
		explicit show_superblock(block_manager::ptr bm)
			: bm_(bm) {
		}

		virtual void exec(strings const &args, ostream &out) {
			if (args.size() > 2)
				throw runtime_error("incorrect number of arguments");

			block_address b = superblock_detail::SUPERBLOCK_LOCATION;
			if (args.size() == 2)
				b = boost::lexical_cast<block_address>(args[1]);
			superblock_detail::superblock sb = read_superblock(bm_, b);

			formatter::ptr f = create_xml_formatter();
			field(*f, "csum", sb.csum_);
			field(*f, "flags", sb.flags_);
			field(*f, "blocknr", sb.blocknr_);
			field(*f, "uuid", sb.uuid_); // FIXME: delimit, and handle non-printable chars
			field(*f, "magic", sb.magic_);
			field(*f, "version", sb.version_);
			field(*f, "time", sb.time_);
			field(*f, "trans_id", sb.trans_id_);
			field(*f, "metadata_snap", sb.metadata_snap_);

			sm_disk_detail::sm_root_disk const *d;
			sm_disk_detail::sm_root v;
			{
				d = reinterpret_cast<sm_disk_detail::sm_root_disk const *>(sb.metadata_space_map_root_);
				sm_disk_detail::sm_root_traits::unpack(*d, v);
				formatter::ptr f2 = create_xml_formatter();
				sm_root_show_traits::show(f2, "value", v);
				f->child("metadata_space_map_root", f2);
			}
			{
				d = reinterpret_cast<sm_disk_detail::sm_root_disk const *>(sb.data_space_map_root_);
				sm_disk_detail::sm_root_traits::unpack(*d, v);
				formatter::ptr f2 = create_xml_formatter();
				sm_root_show_traits::show(f2, "value", v);
				f->child("data_space_map_root", f2);
			}

			field(*f, "data_mapping_root", sb.data_mapping_root_);
			field(*f, "device_details_root", sb.device_details_root_);
			field(*f, "data_block_size", sb.data_block_size_);
			field(*f, "metadata_block_size", sb.metadata_block_size_);
			field(*f, "metadata_nr_blocks", sb.metadata_nr_blocks_);
			field(*f, "compat_flags", sb.compat_flags_);
			field(*f, "compat_ro_flags", sb.compat_ro_flags_);
			field(*f, "incompat_flags", sb.incompat_flags_);

			f->output(out, 0);
		}

	private:
		block_manager::ptr bm_;
	};

	class device_details_show_traits {
	public:
		typedef thin_provisioning::device_tree_detail::device_details_traits value_trait;

		static void show(formatter::ptr f, string const &key,
				 thin_provisioning::device_tree_detail::device_details const &value) {
			field(*f, "mapped_blocks", value.mapped_blocks_);
			field(*f, "transaction_id", value.transaction_id_);
			field(*f, "creation_time", value.creation_time_);
			field(*f, "snap_time", value.snapshotted_time_);
		}
	};

	class block_show_traits {
	public:
		typedef thin_provisioning::mapping_tree_detail::block_traits value_trait;

		static void show(formatter::ptr f, string const &key,
				 thin_provisioning::mapping_tree_detail::block_time const &value) {
			field(*f, "block", value.block_);
			field(*f, "time", value.time_);
		}
	};

	//--------------------------------

	template <typename ShowTraits>
	dbg::command::ptr
	create_btree_node_handler(block_manager::ptr bm) {
		return create_block_handler(bm, create_btree_node_dumper<ShowTraits>());
	}

	dbg::command::ptr
	create_index_block_handler(block_manager::ptr bm) {
		return create_block_handler(bm, create_index_block_dumper());
	}

	int debug_(string const &path) {
		using dbg::command;

		try {
			block_manager::ptr bm = open_bm(path, block_manager::READ_ONLY, 1);
			command_interpreter::ptr interp = create_command_interpreter(cin, cout);
			interp->register_command("hello", create_hello_handler());
			interp->register_command("superblock", command::ptr(new show_superblock(bm)));
			interp->register_command("m1_node", create_btree_node_handler<uint64_show_traits>(bm));
			interp->register_command("m2_node", create_btree_node_handler<block_show_traits>(bm));
			interp->register_command("detail_node", create_btree_node_handler<device_details_show_traits>(bm));
			interp->register_command("index_block", create_index_block_handler(bm));
			interp->register_command("index_node", create_btree_node_handler<index_entry_show_traits>(bm));
			interp->register_command("help", command::ptr(new help));
			interp->register_command("exit", create_exit_handler(interp));
			interp->enter_main_loop();

		} catch (std::exception &e) {
			cerr << e.what() << endl;
			return 1;
		}

		return 0;
	}
}

thin_debug_cmd::thin_debug_cmd()
	: command("thin_debug")
{
}

void
thin_debug_cmd::usage(std::ostream &out) const
{
	out << "Usage: " << get_name() << " {device|file}" << endl
	    << "Options:" << endl
	    << "  {-h|--help}" << endl
	    << "  {-V|--version}" << endl;
}

int
thin_debug_cmd::run(int argc, char **argv)
{
	int c;
	const char shortopts[] = "hV";
	const struct option longopts[] = {
		{ "help", no_argument, NULL, 'h'},
		{ "version", no_argument, NULL, 'V'},
		{ NULL, no_argument, NULL, 0 }
	};

	while ((c = getopt_long(argc, argv, shortopts, longopts, NULL)) != -1) {
		switch(c) {
		case 'h':
			usage(cout);
			return 0;

		case 'V':
			cerr << THIN_PROVISIONING_TOOLS_VERSION << endl;
			return 0;
		}
	}

	if (argc == optind) {
		usage(cerr);
		exit(1);
	}

	return debug_(argv[optind]);
}

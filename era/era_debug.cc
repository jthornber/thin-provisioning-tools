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

#include "dbg-lib/array_block_dumper.h"
#include "dbg-lib/btree_node_dumper.h"
#include "dbg-lib/bitset_block_dumper.h"
#include "dbg-lib/command_interpreter.h"
#include "dbg-lib/commands.h"
#include "dbg-lib/output_formatter.h"
#include "dbg-lib/sm_show_traits.h"
#include "persistent-data/file_utils.h"
#include "persistent-data/space-maps/disk_structures.h"
#include "era/commands.h"
#include "era/metadata.h"
#include "version.h"

using namespace dbg;
using namespace persistent_data;
using namespace std;
using namespace era;

//----------------------------------------------------------------

namespace {
	class help : public dbg::command {
		virtual void exec(strings const &args, ostream &out) {
			out << "Commands:" << endl
			    << "  superblock [block#]" << endl
			    << "  block_node <block# of array block-tree node>" << endl
			    << "  bitset_block <block# of bitset block>" << endl
			    << "  era_block <block# of era array block>" << endl
			    << "  writeset_node <block# of writeset tree node>" << endl
			    << "  exit" << endl;
		}
	};

	// for displaying the writeset tree
	class writeset_show_traits : public era::era_detail_traits {
	public:
		typedef era_detail_traits value_trait;

		static void show(formatter::ptr f, string const &key, era_detail const &value) {
			field(*f, "nr_bits", value.nr_bits);
			field(*f, "writeset_root", value.writeset_root);
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

			block_address b = era::SUPERBLOCK_LOCATION;
			if (args.size() == 2)
				b = boost::lexical_cast<block_address>(args[1]);
			era::superblock sb = read_superblock(bm_, b);

			formatter::ptr f = create_xml_formatter();
			ostringstream version;
			field(*f, "csum", sb.csum);
			field(*f, "flags", sb.flags.encode());
			field(*f, "blocknr", sb.blocknr);
			field(*f, "uuid", sb.uuid); // FIXME: delimit, and handle non-printable chars
			field(*f, "magic", sb.magic);
			field(*f, "version", sb.version);

			sm_disk_detail::sm_root_disk const *d;
			sm_disk_detail::sm_root v;
			{
				d = reinterpret_cast<sm_disk_detail::sm_root_disk const *>(sb.metadata_space_map_root);
				sm_disk_detail::sm_root_traits::unpack(*d, v);
				formatter::ptr f2 = create_xml_formatter();
				sm_root_show_traits::show(f2, "value", v);
				f->child("metadata_space_map_root", f2);
			}

			field(*f, "data_block_size", sb.data_block_size);
			field(*f, "metadata_block_size", sb.metadata_block_size);
			field(*f, "nr_blocks", sb.nr_blocks);

			field(*f, "current_era", sb.current_era);
			{
				formatter::ptr f2 = create_xml_formatter();
				writeset_show_traits::show(f2, "value", sb.current_detail);
				f->child("current_writeset", f2);
			}

			field(*f, "writeset_tree_root", sb.writeset_tree_root);
			field(*f, "era_array_root", sb.era_array_root);

			if (sb.metadata_snap)
				field(*f, "metadata_snap", *sb.metadata_snap);

			f->output(out, 0);
		}

	private:
		block_manager::ptr bm_;
	};

	//--------------------------------

	template <typename ShowTraits>
	dbg::command::ptr
	create_btree_node_handler(block_manager::ptr bm) {
		return create_block_handler(bm, create_btree_node_dumper<ShowTraits>());
	}

	template <typename ShowTraits>
	dbg::command::ptr
	create_array_block_handler(block_manager::ptr bm,
				   typename ShowTraits::value_trait::ref_counter rc) {
		return create_block_handler(bm, create_array_block_dumper<ShowTraits>(rc));
	}

	dbg::command::ptr
	create_bitset_block_handler(block_manager::ptr bm) {
		return create_block_handler(bm, create_bitset_block_dumper());
	}

	int debug(string const &path) {
		using dbg::command;

		try {
			block_manager::ptr bm = open_bm(path, block_manager::READ_ONLY);
			transaction_manager::ptr null_tm = open_tm(bm, era::SUPERBLOCK_LOCATION);
			command_interpreter::ptr interp = create_command_interpreter(cin, cout);
			interp->register_command("hello", create_hello_handler());
			interp->register_command("superblock", command::ptr(new show_superblock(bm)));
			interp->register_command("block_node", create_btree_node_handler<uint64_show_traits>(bm));
			interp->register_command("bitset_block", create_bitset_block_handler(bm));
			interp->register_command("era_block", create_array_block_handler<uint32_show_traits>(bm,
					uint32_traits::ref_counter()));
			interp->register_command("writeset_node", create_btree_node_handler<writeset_show_traits>(bm));
			interp->register_command("help", command::ptr(new help));
			interp->register_command("exit", create_exit_handler(interp));
			interp->enter_main_loop();

		} catch (std::exception &e) {
			cerr << e.what();
			return 1;
		}

		return 0;
	}
}

//----------------------------------------------------------------

era_debug_cmd::era_debug_cmd()
	: command("era_debug")
{
}

void
era_debug_cmd::usage(std::ostream &out) const
{
	out << "Usage: " << get_name() << " {device|file}" << endl
	    << "Options:" << endl
	    << "  {-h|--help}" << endl
	    << "  {-V|--version}" << endl;
}

int
era_debug_cmd::run(int argc, char **argv)
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

	return debug(argv[optind]);
}

//----------------------------------------------------------------

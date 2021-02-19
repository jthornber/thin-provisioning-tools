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
#include "dbg-lib/command_interpreter.h"
#include "dbg-lib/commands.h"
#include "dbg-lib/output_formatter.h"
#include "dbg-lib/sm_show_traits.h"
#include "persistent-data/file_utils.h"
#include "persistent-data/space-maps/disk_structures.h"
#include "caching/commands.h"
#include "caching/metadata.h"
#include "version.h"

using namespace dbg;
using namespace persistent_data;
using namespace std;
using namespace caching;

//----------------------------------------------------------------

namespace {
	class help : public dbg::command {
		virtual void exec(strings const &args, ostream &out) {
			out << "Commands:" << endl
			    << "  superblock [block#]" << endl
			    << "  block_node <block# of array block-tree node>" << endl
			    << "  mapping_block <block# of mappings array block>" << endl
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

			block_address b = caching::SUPERBLOCK_LOCATION;
			if (args.size() == 2)
				b = boost::lexical_cast<block_address>(args[1]);
			caching::superblock sb = read_superblock(bm_, b);

			formatter::ptr f = create_xml_formatter();
			ostringstream version;
			field(*f, "csum", sb.csum);
			field(*f, "flags", sb.flags.encode());
			field(*f, "blocknr", sb.blocknr);
			field(*f, "uuid", sb.uuid); // FIXME: delimit, and handle non-printable chars
			field(*f, "magic", sb.magic);
			field(*f, "version", sb.version);
			field(*f, "policy_name", reinterpret_cast<char const*>(sb.policy_name));
			version << sb.policy_version[0] << "."
				<< sb.policy_version[1] << "."
				<< sb.policy_version[2];
			field(*f, "policy_version", version.str().c_str());
			field(*f, "policy_hint_size", sb.policy_hint_size);

			sm_disk_detail::sm_root_disk const *d;
			sm_disk_detail::sm_root v;
			{
				d = reinterpret_cast<sm_disk_detail::sm_root_disk const *>(sb.metadata_space_map_root);
				sm_disk_detail::sm_root_traits::unpack(*d, v);
				formatter::ptr f2 = create_xml_formatter();
				sm_root_show_traits::show(f2, "value", v);
				f->child("metadata_space_map_root", f2);
			}

			field(*f, "mapping_root", sb.mapping_root);
			if (sb.version >= 2)
				field(*f, "dirty_root", *sb.dirty_root);
			field(*f, "hint_root", sb.hint_root);
			field(*f, "discard_root", sb.discard_root);
			field(*f, "discard_block_size", sb.discard_block_size);
			field(*f, "discard_nr_blocks", sb.discard_nr_blocks);
			field(*f, "data_block_size", sb.data_block_size);
			field(*f, "metadata_block_size", sb.metadata_block_size);
			field(*f, "cache_blocks", sb.cache_blocks);
			field(*f, "compat_flags", sb.compat_flags);
			field(*f, "compat_ro_flags", sb.compat_ro_flags);
			field(*f, "incompat_flags", sb.incompat_flags);
			field(*f, "read_hits", sb.read_hits);
			field(*f, "read_misses", sb.read_misses);
			field(*f, "write_hits", sb.write_hits);
			field(*f, "write_misses", sb.write_misses);

			f->output(out, 0);
		}

	private:
		block_manager::ptr bm_;
	};

	class mapping_show_traits : public caching::mapping_traits {
	public:
		typedef mapping_traits value_trait;

		static void show(formatter::ptr f, string const &key, caching::mapping const &value) {
			field(*f, "oblock", value.oblock_);
			field(*f, "flags", value.flags_);
		}
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

	int debug(string const &path) {
		using dbg::command;

		try {
			block_manager::ptr bm = open_bm(path, block_manager::READ_ONLY);
			command_interpreter::ptr interp = create_command_interpreter(cin, cout);
			interp->register_command("hello", create_hello_handler());
			interp->register_command("superblock", command::ptr(new show_superblock(bm)));
			interp->register_command("block_node", create_btree_node_handler<uint64_show_traits>(bm));
			interp->register_command("mapping_block", create_array_block_handler<mapping_show_traits>(bm,
					mapping_traits::ref_counter()));
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

cache_debug_cmd::cache_debug_cmd()
	: command("cache_debug")
{
}

void
cache_debug_cmd::usage(std::ostream &out) const
{
	out << "Usage: " << get_name() << " {device|file}" << endl
	    << "Options:" << endl
	    << "  {-h|--help}" << endl
	    << "  {-V|--version}" << endl;
}

int
cache_debug_cmd::run(int argc, char **argv)
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

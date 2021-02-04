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

#include "base/command_interpreter.h"
#include "base/output_formatter.h"
#include "persistent-data/file_utils.h"
#include "persistent-data/data-structures/btree.h"
#include "persistent-data/data-structures/simple_traits.h"
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
	class hello : public dbg::command {
		virtual void exec(strings const &args, ostream &out) {
			out << "Hello, world!" << endl;
		}
	};

	class help : public dbg::command {
		virtual void exec(strings const &args, ostream &out) {
			out << "Commands:" << endl
			    << "  superblock" << endl
			    << "  mapping_node <block# of tree node for mappings>" << endl
			    << "  mapping_array <block# of array block for mappings>" << endl
			    << "  exit" << endl;
		}
	};

	class exit_handler : public dbg::command {
	public:
		exit_handler(command_interpreter::ptr interpreter)
			: interpreter_(interpreter) {
		}

		virtual void exec(strings const &args, ostream &out) {
			out << "Goodbye!" << endl;
			interpreter_->exit_main_loop();
		}

		command_interpreter::ptr interpreter_;
	};

	class sm_root_show_traits : public persistent_data::sm_disk_detail::sm_root_traits {
	public:
		static void show(formatter::ptr f, string const &key,
				 persistent_data::sm_disk_detail::sm_root const &value) {
			field(*f, "nr_blocks", value.nr_blocks_);
			field(*f, "nr_allocated", value.nr_allocated_);
			field(*f, "bitmap_root", value.bitmap_root_);
			field(*f, "ref_count_root", value.ref_count_root_);
		}
	};

	class show_superblock : public dbg::command {
	public:
		explicit show_superblock(metadata::ptr md)
			: md_(md) {
		}

		virtual void exec(strings const &args, ostream &out) {
			formatter::ptr f = create_xml_formatter();
			ostringstream version;

			superblock const &sb = md_->sb_;

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
		metadata::ptr md_;
	};

	class uint64_show_traits : public uint64_traits {
	public:
		static void show(formatter::ptr f, string const &key, uint64_t const &value) {
			field(*f, key, boost::lexical_cast<string>(value));
		}
	};

	// FIXME: Expose the array ValueTraits parameter or not?
	//	  Since the block_traits isn't related to the ValueTraits.
	class block_show_traits : public persistent_data::array<caching::mapping_traits>::block_traits {
	public:
		static void show(formatter::ptr f, string const &key, block_address const &value) {
			field(*f, "block", value);
		}
	};

	class mapping_show_traits : public caching::mapping_traits {
	public:
		static void show(formatter::ptr f, string const &key, caching::mapping const &value) {
			field(*f, "oblock", value.oblock_);
			field(*f, "flags", value.flags_);
		}
	};

	template <typename ValueTraits>
	class show_btree_node : public dbg::command {
	public:
		explicit show_btree_node(metadata::ptr md)
			: md_(md) {
		}

		virtual void exec(strings const &args, ostream &out) {
			using namespace persistent_data::btree_detail;

			if (args.size() != 2)
				throw runtime_error("incorrect number of arguments");

			block_address block = boost::lexical_cast<block_address>(args[1]);
			block_manager::read_ref rr = md_->tm_->read_lock(block);

			node_ref<uint64_show_traits> n = btree_detail::to_node<uint64_show_traits>(rr);
			if (n.get_type() == INTERNAL)
				show_node<uint64_show_traits>(n, out);
			else {
				node_ref<ValueTraits> n = btree_detail::to_node<ValueTraits>(rr);
				show_node<ValueTraits>(n, out);
			}
		}

	private:
		template <typename VT>
		void show_node(node_ref<VT> n, ostream &out) {
			formatter::ptr f = create_xml_formatter();

			field(*f, "csum", n.get_checksum());
			field(*f, "blocknr", n.get_location());
			field(*f, "type", n.get_type() == INTERNAL ? "internal" : "leaf");
			field(*f, "nr_entries", n.get_nr_entries());
			field(*f, "max_entries", n.get_max_entries());
			field(*f, "value_size", n.get_value_size());

			for (unsigned i = 0; i < n.get_nr_entries(); i++) {
				formatter::ptr f2 = create_xml_formatter();
				field(*f2, "key", n.key_at(i));
				VT::show(f2, "value", n.value_at(i));
				f->child("child", f2);
			}

			f->output(out, 0);
		}

		metadata::ptr md_;
	};

	template <typename ValueTraits>
	class show_array_block : public dbg::command {
		typedef array_block<ValueTraits, block_manager::read_ref> rblock;
	public:
		explicit show_array_block(metadata::ptr md)
			: md_(md) {
		}

		virtual void exec(strings const& args, ostream &out) {
			if (args.size() != 2)
				throw runtime_error("incorrect number of arguments");

			block_address block = boost::lexical_cast<block_address>(args[1]);
			block_manager::read_ref rr = md_->tm_->read_lock(block);

			rblock b(rr, typename ValueTraits::ref_counter());
			show_array_entries(b, out);
		}

	private:
		void show_array_entries(rblock const& b, ostream &out) {
			formatter::ptr f = create_xml_formatter();
			uint32_t nr_entries = b.nr_entries();

			field(*f, "max_entries", b.max_entries());
			field(*f, "nr_entries", nr_entries);
			field(*f, "value_size", b.value_size());

			for (unsigned i = 0; i < nr_entries; i++) {
				formatter::ptr f2 = create_xml_formatter();
				field(*f2, "index", i);
				ValueTraits::show(f2, "value", b.get(i));
				f->child("child", f2);
			}

			f->output(out, 0);
		}

		metadata::ptr md_;
	};

	//--------------------------------

	int debug(string const &path) {
		using dbg::command;

		try {
			block_manager::ptr bm = open_bm(path, block_manager::READ_ONLY);
			metadata::ptr md(new metadata(bm));
			command_interpreter::ptr interp = create_command_interpreter(cin, cout);
			interp->register_command("hello", command::ptr(new hello));
			interp->register_command("superblock", command::ptr(new show_superblock(md)));
			interp->register_command("mapping_node", command::ptr(new show_btree_node<block_show_traits>(md)));
			interp->register_command("mapping_array", command::ptr(new show_array_block<mapping_show_traits>(md)));
			interp->register_command("help", command::ptr(new help));
			interp->register_command("exit", command::ptr(new exit_handler(interp)));
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

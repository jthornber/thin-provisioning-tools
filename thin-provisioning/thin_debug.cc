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

#include <boost/algorithm/string/classification.hpp>
#include <boost/algorithm/string/split.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/tuple/tuple.hpp>
#include <boost/variant.hpp>
#include <getopt.h>
#include <iostream>
#include <libgen.h>
#include <map>
#include <string>
#include <vector>

#include "persistent-data/data-structures/btree.h"
#include "persistent-data/data-structures/simple_traits.h"
#include "persistent-data/file_utils.h"
#include "persistent-data/space-maps/disk_structures.h"
#include "thin-provisioning/commands.h"
#include "thin-provisioning/metadata.h"
#include "thin-provisioning/metadata_checker.h"
#include "thin-provisioning/superblock.h"
#include "version.h"

using namespace boost;
using namespace persistent_data;
using namespace std;
using namespace thin_provisioning;

namespace {
	typedef vector<string> strings;

	class formatter {
	public:
		typedef std::shared_ptr<formatter> ptr;

		virtual ~formatter() {}

		typedef optional<string> maybe_string;

		void field(string const &name, string const &value) {
			fields_.push_back(field_type(name, value));
		}

		void child(string const &name, formatter::ptr t) {
			fields_.push_back(field_type(name, t));
		}

		virtual void output(ostream &out, int depth = 0, boost::optional<string> name = boost::none) = 0;

	protected:
		typedef boost::variant<string, ptr> value;
		typedef boost::tuple<string, value> field_type;

		vector<field_type> fields_;
	};

	template <typename T>
	void
	field(formatter &t, string const &name, T const &value) {
		t.field(name, lexical_cast<string>(value));
	}

	//--------------------------------

	class xml_formatter : public formatter {
	public:
		virtual void output(ostream &out, int depth, boost::optional<string> name = boost::none)  {
			indent(depth, out);
			if (name && (*name).length())
				out << "<fields id=\"" << *name << "\">" << endl;
			else
				out << "<fields>" << endl;
			vector<field_type>::const_iterator it;
			for (it = fields_.begin(); it != fields_.end(); ++it) {
				if (string const *s = get<string>(&it->get<1>())) {
					indent(depth + 1, out);
					out << "<field key=\""
					    << it->get<0>()
					    << "\" value=\""
					    << *s
					    << "\"/>"
					    << endl;

				} else {
					formatter::ptr f = get<formatter::ptr>(it->get<1>());
					f->output(out, depth + 1, it->get<0>());
				}
			}

			indent(depth, out);
			out << "</fields>" << endl;
		}

	private:
		void indent(int depth, ostream &out) const {
			for (int i = 0; i < depth * 2; i++)
				out << ' ';
		}
	};

	//--------------------------------

	class command {
	public:
		typedef std::shared_ptr<command> ptr;

		virtual ~command() {}
		virtual void exec(strings const &args, ostream &out) = 0;
	};

	class command_interpreter {
	public:
		typedef std::shared_ptr<command_interpreter> ptr;

		command_interpreter(istream &in, ostream &out)
			: in_(in),
			  out_(out),
			  exit_(false) {
		}

		void register_command(string const &str, command::ptr cmd) {
			commands_.insert(make_pair(str, cmd));
		}

		void enter_main_loop() {
			while (!exit_)
				do_once();
		}

		void exit_main_loop() {
			exit_ = true;
		}

	private:
		strings read_input() {
			using namespace boost::algorithm;

			string input;
			getline(in_, input);

			strings toks;
			split(toks, input, is_any_of(" \t"), token_compress_on);

			return toks;
		}

		void do_once() {
			if (in_.eof())
				throw runtime_error("input closed");

			out_ << "> ";
			strings args = read_input();

			map<string, command::ptr>::iterator it;
			it = commands_.find(args[0]);
			if (it == commands_.end())
				out_ << "Unrecognised command" << endl;
			else {
				try {
					it->second->exec(args, out_);
				} catch (std::exception &e) {
					cerr << e.what() << endl;
				}
			}
		}

		istream &in_;
		ostream &out_;
		map <string, command::ptr> commands_;
		bool exit_;
	};

	//--------------------------------

	class hello : public command {
		virtual void exec(strings const &args, ostream &out) {
			out << "Hello, world!" << endl;
		}
	};

	class help : public command {
		virtual void exec(strings const &args, ostream &out) {
			out << "Commands:" << endl
			    << "  superblock" << endl
			    << "  m1_node <block# of top-level mapping tree node>" << endl
			    << "  m2_node <block# of bottom-level mapping tree node>" << endl
			    << "  detail_node <block# of device details tree node>" << endl
			    << "  exit" << endl;
		}
	};

	class exit_handler : public command {
	public:
		exit_handler(command_interpreter &interpreter)
			: interpreter_(interpreter) {
		}

		virtual void exec(strings const &args, ostream &out) {
			out << "Goodbye!" << endl;
			interpreter_.exit_main_loop();
		}

		command_interpreter &interpreter_;
	};

	class sm_root_show_traits : public persistent_data::sm_disk_detail::sm_root_traits {
	public:
		static void show(formatter &f, string const &key,
				 persistent_data::sm_disk_detail::sm_root const &value) {
			field(f, "nr blocks", value.nr_blocks_);
			field(f, "nr allocated", value.nr_allocated_);
			field(f, "bitmap root", value.bitmap_root_);
			field(f, "ref count root", value.ref_count_root_);
		}
	};

	class show_superblock : public command {
	public:
		explicit show_superblock(metadata::ptr md)
			: md_(md) {
		}

		virtual void exec(strings const &args, ostream &out) {
			xml_formatter f;

			thin_provisioning::superblock_detail::superblock const &sb = md_->sb_;

			field(f, "csum", sb.csum_);
			field(f, "flags", sb.flags_);
			field(f, "blocknr", sb.blocknr_);
			field(f, "uuid", sb.uuid_); // FIXME: delimit, and handle non-printable chars
			field(f, "magic", sb.magic_);
			field(f, "version", sb.version_);
			field(f, "time", sb.time_);
			field(f, "trans id", sb.trans_id_);
			field(f, "metadata snap", sb.metadata_snap_);

			sm_disk_detail::sm_root_disk const *d;
			sm_disk_detail::sm_root v;
			{
				d = reinterpret_cast<sm_disk_detail::sm_root_disk const *>(sb.metadata_space_map_root_);
				sm_disk_detail::sm_root_traits::unpack(*d, v);
				formatter::ptr f2(new xml_formatter);
				sm_root_show_traits::show(*f2, "value", v);
				f.child("metadata space map root", f2);
			}
			{
				d = reinterpret_cast<sm_disk_detail::sm_root_disk const *>(sb.data_space_map_root_);
				sm_disk_detail::sm_root_traits::unpack(*d, v);
				formatter::ptr f2(new xml_formatter);
				sm_root_show_traits::show(*f2, "value", v);
				f.child("data space map root", f2);
			}

			field(f, "data mapping root", sb.data_mapping_root_);
			field(f, "device details root", sb.device_details_root_);
			field(f, "data block size", sb.data_block_size_);
			field(f, "metadata block size", sb.metadata_block_size_);
			field(f, "metadata nr blocks", sb.metadata_nr_blocks_);
			field(f, "compat flags", sb.compat_flags_);
			field(f, "compat ro flags", sb.compat_ro_flags_);
			field(f, "incompat flags", sb.incompat_flags_);

			f.output(out, 0);
		}

	private:
		metadata::ptr md_;
	};

	class device_details_show_traits : public thin_provisioning::device_tree_detail::device_details_traits {
	public:
		typedef thin_provisioning::device_tree_detail::device_details_traits value_trait;

		static void show(formatter &f, string const &key,
				 thin_provisioning::device_tree_detail::device_details const &value) {
			field(f, "mapped blocks", value.mapped_blocks_);
			field(f, "transaction id", value.transaction_id_);
			field(f, "creation time", value.creation_time_);
			field(f, "snap time", value.snapshotted_time_);
		}
	};

	class uint64_show_traits : public uint64_traits {
	public:
		typedef uint64_traits value_trait;

		static void show(formatter &f, string const &key, uint64_t const &value) {
			field(f, key, lexical_cast<string>(value));
		}
	};

	class block_show_traits : public thin_provisioning::mapping_tree_detail::block_traits {
	public:
		typedef thin_provisioning::mapping_tree_detail::block_traits value_trait;

		static void show(formatter &f, string const &key,
				 thin_provisioning::mapping_tree_detail::block_time const &value) {
			field(f, "block", value.block_);
			field(f, "time", value.time_);
		}
	};

	template <typename ShowTraits>
	class show_btree_node : public command {
	public:
		explicit show_btree_node(metadata::ptr md)
			: md_(md) {
		}

		virtual void exec(strings const &args, ostream &out) {
			using namespace persistent_data::btree_detail;

			if (args.size() != 2)
				throw runtime_error("incorrect number of arguments");

			block_address block = lexical_cast<block_address>(args[1]);
			block_manager::read_ref rr = md_->tm_->read_lock(block);

			node_ref<uint64_show_traits::value_trait> n = btree_detail::to_node<uint64_show_traits::value_trait>(rr);
			if (n.get_type() == INTERNAL)
				show_node<uint64_show_traits>(n, out);
			else {
				node_ref<typename ShowTraits::value_trait> n = btree_detail::to_node<typename ShowTraits::value_trait>(rr);
				show_node<ShowTraits>(n, out);
			}
		}

	private:
		template <typename ST>
		void show_node(node_ref<typename ST::value_trait> n, ostream &out) {
			xml_formatter f;

			field(f, "csum", n.get_checksum());
			field(f, "blocknr", n.get_location());
			field(f, "type", n.get_type() == INTERNAL ? "internal" : "leaf");
			field(f, "nr entries", n.get_nr_entries());
			field(f, "max entries", n.get_max_entries());
			field(f, "value size", n.get_value_size());

			for (unsigned i = 0; i < n.get_nr_entries(); i++) {
				formatter::ptr f2(new xml_formatter);
				field(*f2, "key", n.key_at(i));
				ST::show(*f2, "value", n.value_at(i));
				f.child("child", f2);
			}

			f.output(out, 0);
		}

		metadata::ptr md_;
	};

	//--------------------------------

	int debug(string const &path, bool ignore_metadata_sm) {
		try {
			block_manager::ptr bm = open_bm(path, block_manager::READ_ONLY, 1);
			metadata::ptr md(new metadata(bm, false));
			command_interpreter interp(cin, cout);
			interp.register_command("hello", command::ptr(new hello));
			interp.register_command("superblock", command::ptr(new show_superblock(md)));
			interp.register_command("m1_node", command::ptr(new show_btree_node<uint64_show_traits>(md)));
			interp.register_command("m2_node", command::ptr(new show_btree_node<block_show_traits>(md)));
			interp.register_command("detail_node", command::ptr(new show_btree_node<device_details_show_traits>(md)));
			interp.register_command("help", command::ptr(new help));
			interp.register_command("exit", command::ptr(new exit_handler(interp)));
			interp.enter_main_loop();

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
		{ "ignore-metadata-sm", no_argument, NULL, 1},
		{ NULL, no_argument, NULL, 0 }
	};
	bool ignore_metadata_sm = false;

	while ((c = getopt_long(argc, argv, shortopts, longopts, NULL)) != -1) {
		switch(c) {
		case 'h':
			usage(cout);
			return 0;

		case 'V':
			cerr << THIN_PROVISIONING_TOOLS_VERSION << endl;
			return 0;

		case 1:
			ignore_metadata_sm = true;
			break;
		}
	}

	if (argc == optind) {
		usage(cerr);
		exit(1);
	}

	return debug(argv[optind], ignore_metadata_sm);
}

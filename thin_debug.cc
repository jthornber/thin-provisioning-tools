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
#include <getopt.h>
#include <iostream>
#include <libgen.h>
#include <map>
#include <string>
#include <vector>

#include "btree.h"
#include "metadata.h"
#include "metadata_checker.h"
#include "version.h"

using namespace boost;
using namespace persistent_data;
using namespace std;
using namespace thin_provisioning;

namespace {
	typedef vector<string> strings;

	class table_formatter {
	public:
		virtual ~table_formatter() {}

		typedef optional<string> maybe_string;

		void field(string const &name, string const &value, maybe_string const &units = maybe_string()) {
			fields_.push_back(field_type(name, value, units));
		}

		virtual void output(ostream &out) = 0;

	protected:
		typedef tuple<string, string, maybe_string> field_type;

		vector<field_type> fields_;
	};

	template <typename T>
	void
	field(table_formatter &t, string const &name, T const &value) {
		t.field(name, lexical_cast<string>(value));
	}

	template <typename T>
	void
	field(table_formatter &t, string const &name, T const &value, string const &units) {
		t.field(name, lexical_cast<string>(value), optional<string>(units));
	}

	//--------------------------------

	class text_formatter : public table_formatter {
	public:
		virtual void output(ostream &out) {
			vector<field_type>::const_iterator it;
			for (it = fields_.begin(); it != fields_.end(); ++it) {
				out << it->get<0>() << ": " << it->get<1>();
				if (it->get<2>())
					out << " " << *(it->get<2>()) << "(s)";
				out << endl;
			}
		}
	};

	//--------------------------------

	class command {
	public:
		typedef boost::shared_ptr<command> ptr;

		virtual ~command() {}
		virtual void exec(strings const &args, ostream &out) = 0;
	};

	class command_interpreter {
	public:
		command_interpreter(istream &in, ostream &out)
			: in_(in),
			  out_(out) {
		}

		void register_command(string const &str, command::ptr cmd) {
			commands_.insert(make_pair(str, cmd));
		}

		void enter_main_loop() {
			while (true)
				do_once();
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
			else
				it->second->exec(args, out_);
		}

		istream &in_;
		ostream &out_;
		map <string, command::ptr> commands_;
	};

	//--------------------------------

	class hello : public command {
		virtual void exec(strings const &args, ostream &out) {
			out << "Hello, world!" << endl;
		}
	};

	class show_superblock : public command {
	public:
		explicit show_superblock(metadata::ptr md)
			: md_(md) {
		}

		virtual void exec(strings const &args, ostream &out) {
			text_formatter f;

			superblock const &sb = md_->sb_;

			field(f, "csum", sb.csum_);
			field(f, "flags", sb.flags_);
			field(f, "blocknr", sb.blocknr_);
			field(f, "uuid", sb.uuid_); // FIXME: delimit, and handle non-printable chars
			field(f, "magic", sb.magic_);
			field(f, "version", sb.version_);
			field(f, "time", sb.time_);
			field(f, "trans id", sb.trans_id_);
			field(f, "held root", sb.held_root_);
			field(f, "data mapping root", sb.data_mapping_root_);
			field(f, "device details root", sb.device_details_root_);
			field(f, "data block size", sb.data_block_size_, "sector");
			field(f, "metadata block size", sb.metadata_block_size_, "sector");
			field(f, "metadata nr blocks", sb.metadata_nr_blocks_);
			field(f, "compat flags", sb.compat_flags_);
			field(f, "compat ro flags", sb.compat_ro_flags_);
			field(f, "incompat flags", sb.incompat_flags_);

			f.output(out);
		}

	private:
		metadata::ptr md_;
	};

	template <typename ValueTraits>
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
			block_manager<>::read_ref rr = md_->tm_->read_lock(block);
			node_ref<ValueTraits> n = btree_detail::to_node<ValueTraits>(rr);

			text_formatter f;

			field(f, "csum", n.get_checksum());
			field(f, "blocknr", n.get_location());
			field(f, "type", n.get_type() == INTERNAL ? "internal" : "leaf");
			field(f, "nr entries", n.get_nr_entries());
			field(f, "max entries", n.get_max_entries());
			field(f, "value size", n.get_value_size());

			f.output(out);
		}

	private:
		metadata::ptr md_;
	};

	//--------------------------------

	int debug(string const &path) {
		try {
			metadata::ptr md(new metadata(path, metadata::OPEN));
			command_interpreter interp(cin, cout);
			interp.register_command("hello", command::ptr(new hello));
			interp.register_command("superblock", command::ptr(new show_superblock(md)));
			interp.register_command("btree_node", command::ptr(new show_btree_node<device_details_traits>(md)));
			interp.enter_main_loop();

		} catch (std::exception &e) {
			cerr << e.what();
			return 1;
		}

		return 0;
	}

	void usage(string const &cmd) {
		cerr << "Usage: " << cmd << " {device|file}" << endl
		     << "Options:" << endl
		     << "  {-h|--help}" << endl
		     << "  {-V|--version}" << endl;
	}
}

int main(int argc, char **argv)
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
			usage(basename(argv[0]));
			return 0;

		case 'V':
			cerr << THIN_PROVISIONING_TOOLS_VERSION << endl;
			return 0;
		}
	}

	if (argc == optind) {
		usage(basename(argv[0]));
		exit(1);
	}

	return debug(argv[optind]);
}

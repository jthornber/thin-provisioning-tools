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
#include "era/commands.h"
#include "era/metadata.h"
#include "version.h"

using namespace dbg;
using namespace persistent_data;
using namespace std;
using namespace era;

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
			    << "  superblock [block#]" << endl
			    << "  block_node <block# of array block-tree node>" << endl
			    << "  bitset_block <block# of bitset block>" << endl
			    << "  era_block <block# of era array block>" << endl
			    << "  writeset_node <block# of writeset tree node>" << endl
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

	// FIXME: duplication
	class uint32_show_traits : public uint32_traits {
	public:
		typedef uint32_traits value_trait;

		static void show(formatter::ptr f, string const &key, uint32_t const &value) {
			field(*f, key, boost::lexical_cast<string>(value));
		}
	};

	// FIXME: duplication
	class uint64_show_traits : public uint64_traits {
	public:
		typedef uint64_traits value_trait;

		static void show(formatter::ptr f, string const &key, uint64_t const &value) {
			field(*f, key, boost::lexical_cast<string>(value));
		}
	};

	// FIXME: duplication
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

	// FIXME: duplication
	template <typename ShowTraits>
	class show_btree_node : public dbg::command {
	public:
		explicit show_btree_node(block_manager::ptr bm)
			: bm_(bm) {
		}

		virtual void exec(strings const &args, ostream &out) {
			using namespace persistent_data::btree_detail;

			if (args.size() != 2)
				throw runtime_error("incorrect number of arguments");

			block_address block = boost::lexical_cast<block_address>(args[1]);
			block_manager::read_ref rr = bm_->read_lock(block);

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
			formatter::ptr f = create_xml_formatter();

			field(*f, "csum", n.get_checksum());
			field(*f, "blocknr", n.get_block_nr());
			field(*f, "type", n.get_type() == INTERNAL ? "internal" : "leaf");
			field(*f, "nr_entries", n.get_nr_entries());
			field(*f, "max_entries", n.get_max_entries());
			field(*f, "value_size", n.get_value_size());

			for (unsigned i = 0; i < n.get_nr_entries(); i++) {
				formatter::ptr f2 = create_xml_formatter();
				field(*f2, "key", n.key_at(i));
				ST::show(f2, "value", n.value_at(i));
				f->child(boost::lexical_cast<string>(i), f2);
			}

			f->output(out, 0);
		}

		block_manager::ptr bm_;
	};

	// FIXME: duplication
	template <typename ShowTraits>
	class show_array_block : public dbg::command {
		typedef array_block<typename ShowTraits::value_trait, block_manager::read_ref> rblock;
	public:
		explicit show_array_block(block_manager::ptr bm,
					  typename ShowTraits::ref_counter rc)
			: bm_(bm), rc_(rc) {
		}

		virtual void exec(strings const& args, ostream &out) {
			if (args.size() != 2)
				throw runtime_error("incorrect number of arguments");

			block_address block = boost::lexical_cast<block_address>(args[1]);
			block_manager::read_ref rr = bm_->read_lock(block);

			rblock b(rr, rc_);
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
				ShowTraits::show(f2, "value", b.get(i));
				f->child(boost::lexical_cast<string>(i), f2);
			}

			f->output(out, 0);
		}

		block_manager::ptr bm_;
		typename ShowTraits::ref_counter rc_;
	};

	// FIXME: duplication
	class show_bitset_block : public dbg::command {
		typedef array_block<uint64_traits, block_manager::read_ref> rblock;
	public:
		explicit show_bitset_block(block_manager::ptr bm)
			: bm_(bm),
			  BITS_PER_ARRAY_ENTRY(64) {
		}

		virtual void exec(strings const& args, ostream &out) {
			if (args.size() != 2)
				throw runtime_error("incorrect number of arguments");

			block_address block = boost::lexical_cast<block_address>(args[1]);
			block_manager::read_ref rr = bm_->read_lock(block);

			rblock b(rr, rc_);
			show_bitset_entries(b, out);
		}

	private:
		void show_bitset_entries(rblock const& b, ostream &out) {
			formatter::ptr f = create_xml_formatter();
			uint32_t nr_entries = b.nr_entries();

			field(*f, "max_entries", b.max_entries());
			field(*f, "nr_entries", nr_entries);
			field(*f, "value_size", b.value_size());

			uint32_t end_pos = b.nr_entries() * BITS_PER_ARRAY_ENTRY;
			std::pair<uint32_t, uint32_t> range = next_set_bits(b, 0);
			for (; range.first < end_pos; range = next_set_bits(b, range.second)) {
				formatter::ptr f2 = create_xml_formatter();
				field(*f2, "begin", range.first);
				field(*f2, "end", range.second);
				f->child("set_bits", f2);
			}

			f->output(out, 0);
		}

		// Returns the range of set bits, starts from the offset.
		pair<uint32_t, uint32_t> next_set_bits(rblock const &b, uint32_t offset) {
			uint32_t end_pos = b.nr_entries() * BITS_PER_ARRAY_ENTRY;
			uint32_t begin = find_first_set(b, offset);

			if (begin == end_pos) // not found
				return make_pair(end_pos, end_pos);

			uint32_t end = find_first_unset(b, begin + 1);
			return make_pair(begin, end);
		}

		// Returns the position (zero-based) of the first bit set
		// in the array block, starts from the offset.
		// Returns the pass-the-end position if not found.
		uint32_t find_first_set(rblock const &b, uint32_t offset) {
			uint32_t entry = offset / BITS_PER_ARRAY_ENTRY;
			uint32_t nr_entries = b.nr_entries();

			if (entry >= nr_entries)
				return entry * BITS_PER_ARRAY_ENTRY;

			uint32_t idx = offset % BITS_PER_ARRAY_ENTRY;
			uint64_t v = b.get(entry++) >> idx;
			while (!v && entry < nr_entries) {
				v = b.get(entry++);
				idx = 0;
			}

			if (!v) // not found
				return entry * BITS_PER_ARRAY_ENTRY;

			return (entry - 1) * BITS_PER_ARRAY_ENTRY + idx + ffsll(static_cast<long long>(v)) - 1;
		}

		// Returns the position (zero-based) of the first zero bit
		// in the array block, starts from the offset.
		// Returns the pass-the-end position if not found.
		// FIXME: improve efficiency
		uint32_t find_first_unset(rblock const& b, uint32_t offset) {
			uint32_t entry = offset / BITS_PER_ARRAY_ENTRY;
			uint32_t nr_entries = b.nr_entries();

			if (entry >= nr_entries)
				return entry * BITS_PER_ARRAY_ENTRY;

			uint32_t idx = offset % BITS_PER_ARRAY_ENTRY;
			uint64_t v = b.get(entry++);
			while (all_bits_set(v, idx) && entry < nr_entries) {
				v = b.get(entry++);
				idx = 0;
			}

			if (all_bits_set(v, idx)) // not found
				return entry * BITS_PER_ARRAY_ENTRY;

			return (entry - 1) * BITS_PER_ARRAY_ENTRY + idx + count_leading_bits(v, idx);
		}

		// Returns true if all the bits beyond the position are set.
		bool all_bits_set(uint64_t v, uint32_t offset) {
			return (v >> offset) == (numeric_limits<uint64_t>::max() >> offset);
		}

		// Counts the number of leading 1's in the given value, starts from the offset
		// FIXME: improve efficiency
		uint32_t count_leading_bits(uint64_t v, uint32_t offset) {
			uint32_t count = 0;

			v >>= offset;
			while (v & 0x1) {
				v >>= 1;
				count++;
			}

			return count;
		}

		block_manager::ptr bm_;
		uint64_traits::ref_counter rc_;

		const uint32_t BITS_PER_ARRAY_ENTRY;
	};

	//--------------------------------

	int debug(string const &path) {
		using dbg::command;

		try {
			block_manager::ptr bm = open_bm(path, block_manager::READ_ONLY);
			transaction_manager::ptr null_tm = open_tm(bm, era::SUPERBLOCK_LOCATION);
			command_interpreter::ptr interp = create_command_interpreter(cin, cout);
			interp->register_command("hello", command::ptr(new hello));
			interp->register_command("superblock", command::ptr(new show_superblock(bm)));
			interp->register_command("block_node", command::ptr(new show_btree_node<uint64_show_traits>(bm)));
			interp->register_command("bitset_block", command::ptr(new show_bitset_block(bm)));
			interp->register_command("era_block", command::ptr(new show_array_block<uint32_show_traits>(bm,
					uint32_show_traits::ref_counter())));
			interp->register_command("writeset_node", command::ptr(new show_btree_node<writeset_show_traits>(bm)));
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

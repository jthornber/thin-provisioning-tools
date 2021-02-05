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
#include "base/math_utils.h"
#include "base/output_formatter.h"
#include "persistent-data/data-structures/btree.h"
#include "persistent-data/data-structures/simple_traits.h"
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
	class hello : public dbg::command {
		virtual void exec(strings const &args, ostream &out) {
			out << "Hello, world!" << endl;
		}
	};

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

	class device_details_show_traits : public thin_provisioning::device_tree_detail::device_details_traits {
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

	class uint64_show_traits : public uint64_traits {
	public:
		typedef uint64_traits value_trait;

		static void show(formatter::ptr f, string const &key, uint64_t const &value) {
			field(*f, key, boost::lexical_cast<string>(value));
		}
	};

	class block_show_traits : public thin_provisioning::mapping_tree_detail::block_traits {
	public:
		typedef thin_provisioning::mapping_tree_detail::block_traits value_trait;

		static void show(formatter::ptr f, string const &key,
				 thin_provisioning::mapping_tree_detail::block_time const &value) {
			field(*f, "block", value.block_);
			field(*f, "time", value.time_);
		}
	};

	class index_entry_show_traits : public persistent_data::sm_disk_detail::index_entry_traits {
	public:
		typedef persistent_data::sm_disk_detail::index_entry_traits value_trait;

		static void show(formatter::ptr f, string const &key,
				 persistent_data::sm_disk_detail::index_entry const &value) {
			field(*f, "blocknr", value.blocknr_);
			field(*f, "nr_free", value.nr_free_);
			field(*f, "none_free_before", value.none_free_before_);
		}
	};

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

	class show_index_block : public dbg::command {
	public:
		explicit show_index_block(block_manager::ptr bm)
			: bm_(bm) {
		}

		virtual void exec(strings const &args, ostream &out) {
			if (args.size() != 2)
				throw runtime_error("incorrect number of arguments");

			block_address block = boost::lexical_cast<block_address>(args[1]);

			// no checksum validation for debugging purpose
			block_manager::read_ref rr = bm_->read_lock(block);

			sm_disk_detail::metadata_index const *mdi =
				reinterpret_cast<sm_disk_detail::metadata_index const *>(rr.data());
			show_metadata_index(mdi, sm_disk_detail::MAX_METADATA_BITMAPS, out);
		}

	private:
		void show_metadata_index(sm_disk_detail::metadata_index const *mdi, block_address nr_indexes, ostream &out) {
			formatter::ptr f = create_xml_formatter();
			field(*f, "csum", to_cpu<uint32_t>(mdi->csum_));
			field(*f, "padding", to_cpu<uint32_t>(mdi->padding_));
			field(*f, "blocknr", to_cpu<uint64_t>(mdi->blocknr_));

			sm_disk_detail::index_entry ie;
			for (block_address i = 0; i < nr_indexes; i++) {
				sm_disk_detail::index_entry_traits::unpack(*(mdi->index + i), ie);

				if (!ie.blocknr_ && !ie.nr_free_ && !ie.none_free_before_)
					continue;

				formatter::ptr f2 = create_xml_formatter();
				index_entry_show_traits::show(f2, "value", ie);
				f->child(boost::lexical_cast<string>(i), f2);
			}
			f->output(out, 0);
		}

		unsigned const ENTRIES_PER_BLOCK = (MD_BLOCK_SIZE - sizeof(persistent_data::sm_disk_detail::bitmap_header)) * 4;
		block_manager::ptr bm_;
	};

	//--------------------------------

	int debug_(string const &path) {
		using dbg::command;

		try {
			block_manager::ptr bm = open_bm(path, block_manager::READ_ONLY, 1);
			command_interpreter::ptr interp = create_command_interpreter(cin, cout);
			interp->register_command("hello", command::ptr(new hello));
			interp->register_command("superblock", command::ptr(new show_superblock(bm)));
			interp->register_command("m1_node", command::ptr(new show_btree_node<uint64_show_traits>(bm)));
			interp->register_command("m2_node", command::ptr(new show_btree_node<block_show_traits>(bm)));
			interp->register_command("detail_node", command::ptr(new show_btree_node<device_details_show_traits>(bm)));
			interp->register_command("index_block", command::ptr(new show_index_block(bm)));
			interp->register_command("index_node", command::ptr(new show_btree_node<index_entry_show_traits>(bm)));
			interp->register_command("help", command::ptr(new help));
			interp->register_command("exit", command::ptr(new exit_handler(interp)));
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

// Copyright (C) 2015 Red Hat, Inc. All rights reserved.
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

#include <fstream>
#include <iostream>
#include <getopt.h>
#include <libgen.h>

#include "human_readable_format.h"
#include "metadata_dumper.h"
#include "metadata.h"
#include "xml_format.h"
#include "version.h"
#include "thin-provisioning/commands.h"
#include "persistent-data/file_utils.h"
#include "boost/optional.hpp"

using namespace boost;
using namespace persistent_data;
using namespace std;
using namespace thin_provisioning;


//----------------------------------------------------------------

namespace {
	class mapping_set {
	public:
		mapping_set(block_address nr_blocks)
		: bits_(nr_blocks * 2, false) {
		}

		enum block_state {
			UNMAPPED,
			EXCLUSIVE,
			SHARED
		};

		void inc(block_address b) {
			if (bits_[b * 2])
				bits_[b * 2 + 1] = true; // shared
			else
				bits_[b * 2] = true; // exclusive
		}

		block_state get_state(block_address b) const {
			if (bits_[b * 2]) {
				if (bits_[b * 2 + 1])
					return SHARED;
				else
					return EXCLUSIVE;
			} else
				return UNMAPPED;
		}

	private:
		vector<bool> bits_;
	};

	//------------------------------------------------

	struct flags {
		flags()
			: use_metadata_snap(false) {
		}

		bool use_metadata_snap;
	};

	class mapping_pass1 : public mapping_tree_detail::mapping_visitor {
	public:
		mapping_pass1(mapping_set &mappings)
		: mappings_(mappings) {
		}

		virtual void visit(btree_path const &path, mapping_tree_detail::block_time const &bt) {
			mappings_.inc(bt.block_);
		}

	private:
		mapping_set &mappings_;
	};

	class mapping_pass2 : public mapping_tree_detail::mapping_visitor {
	public:
		mapping_pass2(mapping_set const &mappings)
		: mappings_(mappings),
		  exclusives_(0) {
		}

		virtual void visit(btree_path const &path, mapping_tree_detail::block_time const &bt) {
			if (mappings_.get_state(bt.block_) == mapping_set::EXCLUSIVE)
				exclusives_++;
		}

		block_address get_exclusives() const {
			return exclusives_;
		}

	private:
		mapping_set const &mappings_;
		block_address exclusives_;
	};

	void raise_metadata_damage() {
		throw std::runtime_error("metadata contains errors (run thin_check for details).");
	}

	class fatal_mapping_damage : public mapping_tree_detail::damage_visitor {
	public:
		virtual void visit(mapping_tree_detail::missing_devices const &d) {
			raise_metadata_damage();
		}

		virtual void visit(mapping_tree_detail::missing_mappings const &d) {
			raise_metadata_damage();
		}
	};

	void pass1(metadata::ptr md, mapping_set &mappings, uint64_t dev_id) {
		dev_tree::key k = {dev_id};
		optional<uint64_t> dev_root = md->mappings_top_level_->lookup(k);

		if (!dev_root)
			throw runtime_error("couldn't find mapping tree root");

		single_mapping_tree dev_mappings(*md->tm_, *dev_root,
				   mapping_tree_detail::block_traits::ref_counter(md->tm_->get_sm()));

		mapping_pass1 pass1(mappings);
		fatal_mapping_damage dv;
		walk_mapping_tree(dev_mappings, pass1, dv);
	}


	block_address count_exclusives(metadata::ptr md, mapping_set const &mappings, uint64_t dev_id) {
		dev_tree::key k = {dev_id};
		optional<uint64_t> dev_root = md->mappings_top_level_->lookup(k);

		if (!dev_root)
			throw runtime_error("couldn't find mapping tree root");

		single_mapping_tree dev_mappings(*md->tm_, *dev_root,
				   mapping_tree_detail::block_traits::ref_counter(md->tm_->get_sm()));

		mapping_pass2 pass2(mappings);
		fatal_mapping_damage dv;
		walk_mapping_tree(dev_mappings, pass2, dv);
		return pass2.get_exclusives();
	}

	//------------------------------------------------

 	typedef map<block_address, device_tree_detail::device_details> dd_map;

	class details_extractor : public device_tree_detail::device_visitor {
	public:
		void visit(block_address dev_id, device_tree_detail::device_details const &dd) {
			dd_.insert(make_pair(dev_id, dd));
		}

		dd_map const &get_details() const {
			return dd_;
		}

	private:
		dd_map dd_;
	};

	struct fatal_details_damage : public device_tree_detail::damage_visitor {
		void visit(device_tree_detail::missing_devices const &d) {
			raise_metadata_damage();
		}
	};

	device_tree_detail::damage_visitor::ptr details_damage_policy() {
		typedef device_tree_detail::damage_visitor::ptr dvp;
		return dvp(new fatal_details_damage());
	}

	//------------------------------------------------

	int ls_(string const &path, ostream &out, struct flags &flags) {
		try {
			block_manager<>::ptr bm(open_bm(path, block_manager<>::READ_ONLY));
			metadata::ptr md(new metadata(bm));

			details_extractor de;
			device_tree_detail::damage_visitor::ptr dd_policy(details_damage_policy());
			walk_device_tree(*md->details_, de, *dd_policy);

			mapping_set mappings(md->data_sm_->get_nr_blocks());

			dd_map const &map = de.get_details();
			dd_map::const_iterator it;
			for (it = map.begin(); it != map.end(); ++it)
				pass1(md, mappings, it->first);

			for (it = map.begin(); it != map.end(); ++it) {
				block_address exclusive = count_exclusives(md, mappings, it->first);

				out << it->first << ": "
				    << it->second.mapped_blocks_ << " mapped blocks, "
				    << exclusive << " exclusive blocks\n";
			}

		} catch (std::exception &e) {
			cerr << e.what() << endl;
			return 1;
		}

		return 0;
	}

	int ls(string const &path, struct flags &flags) {
		return ls_(path, cout, flags);
	}

	void usage(ostream &out, string const &cmd) {
	}
}

//----------------------------------------------------------------

thin_ls_cmd::thin_ls_cmd()
	: command("thin_ls")
{
}

void
thin_ls_cmd::usage(std::ostream &out) const
{
	out << "Usage: " << get_name() << " [options] {device|file}" << endl
	    << "Options:" << endl
	    << "  {-h|--help}" << endl
	    << "  {-m|--metadata-snap}" << endl
	    << "  {-V|--version}" << endl;
}

int
thin_ls_cmd::run(int argc, char **argv)
{
	int c;
	struct flags flags;
	const char shortopts[] = "hm::V";

	const struct option longopts[] = {
		{ "help", no_argument, NULL, 'h'},
		{ "metadata-snap", no_argument, NULL, 'm' },
		{ "version", no_argument, NULL, 'V'},
		{ NULL, no_argument, NULL, 0 }
	};

	while ((c = getopt_long(argc, argv, shortopts, longopts, NULL)) != -1) {
		switch(c) {
		case 'h':
			usage(cout);
			return 0;

		case 'm':
			flags.use_metadata_snap = true;
			break;

		case 'V':
			cout << THIN_PROVISIONING_TOOLS_VERSION << endl;
			return 0;

		default:
			usage(cerr);
			return 1;
		}
	}

	if (argc == optind) {
		cerr << "No input file provided." << endl;
		usage(cerr);
		return 1;
	}

	return ls(argv[optind], flags);
}

//----------------------------------------------------------------

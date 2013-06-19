// Copyright (C) 2011 Red Hat, Inc. All rights reserved.
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

#include <iostream>
#include <getopt.h>
#include <libgen.h>

#include "version.h"

#include "persistent-data/space-maps/core.h"
#include "thin-provisioning/device_tree.h"
#include "thin-provisioning/file_utils.h"
#include "thin-provisioning/mapping_tree.h"
#include "thin-provisioning/superblock.h"

using namespace std;
using namespace thin_provisioning;

//----------------------------------------------------------------

namespace {

	class end_message {};

	class nested_output {
	public:
		nested_output(ostream &out, unsigned step)
			: out_(out),
			  step_(step),
			  beginning_of_line_(true),
			  indent_(0) {
		}

		template <typename T>
		nested_output &operator <<(T const &t) {
			if (beginning_of_line_) {
				beginning_of_line_ = false;
				indent();
			}

			out_ << t;
			return *this;
		}

		nested_output &operator <<(end_message const &m) {
			beginning_of_line_ = true;
			out_ << endl;
			return *this;
		}

		void inc_indent() {
			indent_ += step_;
		}

		void dec_indent() {
			indent_ -= step_;
		}

		struct nest {
			nest(nested_output &out)
			: out_(out) {
				out_.inc_indent();
			}

			~nest() {
				out_.dec_indent();
			}

			nested_output &out_;
		};

		nest push() {
			return nest(*this);
		}

	private:
		void indent() {
			for (unsigned i = 0; i < indent_; i++)
				out_ << ' ';
		}

		ostream &out_;
		unsigned step_;

		bool beginning_of_line_;
		unsigned indent_;
	};

	//--------------------------------

	enum error_state {
		NO_ERROR,
		NON_FATAL,	// eg, lost blocks
		FATAL		// needs fixing before pool can be activated
	};

	error_state
	combine_errors(error_state lhs, error_state rhs) {
		switch (lhs) {
		case NO_ERROR:
			return rhs;

		case NON_FATAL:
			return (rhs == FATAL) ? FATAL : lhs;

		default:
			return lhs;
		}
	}

	//--------------------------------

	block_manager<>::ptr
	open_bm(string const &path) {
		block_address nr_blocks = get_nr_blocks(path);
		typename block_io<>::mode m = block_io<>::READ_ONLY;
		return block_manager<>::ptr(new block_manager<>(path, nr_blocks, 1, m));
	}

	transaction_manager::ptr
	open_tm(block_manager<>::ptr bm) {
		space_map::ptr sm(new core_map(bm->get_nr_blocks()));
		sm->inc(superblock_detail::SUPERBLOCK_LOCATION);
		transaction_manager::ptr tm(new transaction_manager(bm, sm));
		return tm;
	}

	//--------------------------------

	class superblock_reporter : public superblock_detail::damage_visitor {
	public:
		superblock_reporter(nested_output &out)
		: out_(out),
		  err_(NO_ERROR) {
		}

		virtual void visit(superblock_detail::superblock_corruption const &d) {
			out_ << "superblock is corrupt" << end_message();
			{
				auto _ = out_.push();
				out_ << d.desc_ << end_message();
			}
			err_ = combine_errors(err_, FATAL);
		}

		error_state get_error() const {
			return err_;
		}

	private:
		nested_output &out_;
		error_state err_;
	};

	//--------------------------------

	class devices_reporter : public device_tree_detail::damage_visitor {
	public:
		devices_reporter(nested_output &out)
		: out_(out),
		  err_(NO_ERROR) {
		}

		virtual void visit(device_tree_detail::missing_devices const &d) {
			out_ << "missing devices: " << d.keys_ << end_message();
			{
				auto _ = out_.push();
				out_ << d.desc_ << end_message();
			}

			err_ = combine_errors(err_, FATAL);
		}

		error_state get_error() const {
			return err_;
		}

	private:
		nested_output &out_;
		error_state err_;
	};

	//--------------------------------

	class mapping_reporter : public mapping_tree_detail::damage_visitor {
	public:
		mapping_reporter(nested_output &out)
		: out_(out) {
		}

		virtual void visit(mapping_tree_detail::missing_devices const &d) {
			out_ << "missing all mappings for devices: " << d.keys_ << end_message();
			{
				auto _ = out_.push();
				out_ << d.desc_ << end_message();
			}
			err_ = combine_errors(err_, FATAL);
		}

		virtual void visit(mapping_tree_detail::missing_mappings const &d) {
			out_ << "thin device " << d.thin_dev_ << " is missing mappings " << d.keys_ << end_message();
			{
				auto _ = out_.push();
				out_ << d.desc_ << end_message();
			}
			err_ = combine_errors(err_, FATAL);
		}

	private:
		nested_output &out_;
		error_state err_;
	};

	//--------------------------------

	struct flags {
		bool check_device_tree;
		bool check_mapping_tree_level1;
		bool check_mapping_tree_level2;

		bool ignore_non_fatal_errors;

		bool quiet;
	};

	error_state metadata_check(string const &path, flags fs) {
		block_manager<>::ptr bm = open_bm(path);

		nested_output out(cerr, 2);
		superblock_reporter sb_rep(out);
		devices_reporter dev_rep(out);
		mapping_reporter mapping_rep(out);

		out << "examining superblock" << end_message();
		{
			auto _ = out.push();
			check_superblock(bm, sb_rep);
		}

		if (sb_rep.get_error() == FATAL)
			return FATAL;

		superblock_detail::superblock sb = read_superblock(bm);
		transaction_manager::ptr tm = open_tm(bm);

		if (fs.check_device_tree) {
			out << "examining devices tree" << end_message();
			{
				auto _ = out.push();
				device_tree dtree(tm, sb.device_details_root_,
						  device_tree_detail::device_details_traits::ref_counter());
				check_device_tree(dtree, dev_rep);
			}
		}

		if (fs.check_mapping_tree_level1 && !fs.check_mapping_tree_level2) {
			out << "examining top level of mapping tree" << end_message();
			{
				auto _ = out.push();
				dev_tree dtree(tm, sb.data_mapping_root_,
					       mapping_tree_detail::mtree_traits::ref_counter(tm));
				check_mapping_tree(dtree, mapping_rep);
			}

		} else if (fs.check_mapping_tree_level2) {
			out << "examining mapping tree" << end_message();
			{
				auto _ = out.push();
				mapping_tree mtree(tm, sb.data_mapping_root_,
						   mapping_tree_detail::block_traits::ref_counter(tm->get_sm()));
				check_mapping_tree(mtree, mapping_rep);
			}
		}

		return combine_errors(sb_rep.get_error(),
				      dev_rep.get_error());
	}

	int check(string const &path, flags fs) {
		error_state err;

		try {
			err = metadata_check(path, fs);

		} catch (std::exception &e) {
			if (!fs.quiet)
				cerr << e.what() << endl;

			return 1;
		}

		if (fs.ignore_non_fatal_errors)
			return (err == FATAL) ? 1 : 0;
		else
			return (err == NO_ERROR) ? 0 : 1;
	}

	void usage(ostream &out, string const &cmd) {
		out << "Usage: " << cmd << " [options] {device|file}" << endl
		    << "Options:" << endl
		    << "  {-q|--quiet}" << endl
		    << "  {-h|--help}" << endl
		    << "  {-V|--version}" << endl
		    << "  {--super-block-only}" << endl
		    << "  {--skip-mappings}" << endl
		    << "  {--ignore-non-fatal-errors}" << endl;
	}
}

int main(int argc, char **argv)
{
	int c;
	flags fs;
	char const shortopts[] = "qhV";
	option const longopts[] = {
		{ "quiet", no_argument, NULL, 'q'},
		{ "help", no_argument, NULL, 'h'},
		{ "version", no_argument, NULL, 'V'},
		{ "super-block-only", no_argument, NULL, 1},
		{ "skip-mappings", no_argument, NULL, 2},
		{ "ignore-non-fatal-errors", no_argument, NULL, 3},
		{ NULL, no_argument, NULL, 0 }
	};

	fs.check_device_tree = true;
	fs.check_mapping_tree_level1 = true;
	fs.check_mapping_tree_level2 = true;
	fs.ignore_non_fatal_errors = false;
	fs.quiet = false;

	while ((c = getopt_long(argc, argv, shortopts, longopts, NULL)) != -1) {
		switch(c) {
		case 'h':
			usage(cout, basename(argv[0]));
			return 0;

		case 'q':
			fs.quiet = true;
			break;

		case 'V':
			cout << THIN_PROVISIONING_TOOLS_VERSION << endl;
			return 0;

		case 1:
			// super-block-only
			fs.check_device_tree = false;
			fs.check_mapping_tree_level1 = false;
			fs.check_mapping_tree_level2 = false;
			break;

		case 2:
			// skip-mappings
			fs.check_mapping_tree_level2 = false;
			break;

		case 3:
			// ignore-non-fatal-errors
			fs.ignore_non_fatal_errors = true;
			break;

		default:
			usage(cerr, basename(argv[0]));
			return 1;
		}
	}

	if (argc == optind) {
		if (!fs.quiet) {
			cerr << "No input file provided." << endl;
			usage(cerr, basename(argv[0]));
		}

		exit(1);
	}

	return check(argv[optind], fs);
}

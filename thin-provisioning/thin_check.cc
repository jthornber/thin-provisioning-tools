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

#include "thin-provisioning/device_tree.h"
#include "thin-provisioning/file_utils.h"
#include "thin-provisioning/mapping_tree.h"
#include "thin-provisioning/superblock.h"

using namespace persistent_data;
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

	block_manager<>::ptr open_bm(string const &path) {
		block_address nr_blocks = get_nr_blocks(path);
		typename block_io<>::mode m = block_io<>::READ_ONLY;
		return block_manager<>::ptr(new block_manager<>(path, nr_blocks, 1, m));
	}

	class superblock_reporter : public superblock_detail::damage_visitor {
	public:
		superblock_reporter(nested_output &out)
		: out_(out),
		  err_(NO_ERROR) {
		}

		virtual void visit(superblock_detail::superblock_corruption const &d) {
			out_ << "superblock is corrupt" << end_message();
			err_ = combine_errors(err_, FATAL);
		}

		error_state get_error() const {
			return err_;
		}

	private:
		nested_output &out_;
		error_state err_;
	};

	error_state metadata_check(string const &path) {
		block_manager<>::ptr bm = open_bm(path);

		nested_output out(cerr, 2);
		out << "examining superblock" << end_message();
		{
			auto _ = out.push();
			superblock_reporter sb_rep(out);
			check_superblock(bm, sb_rep);

			return sb_rep.get_error();
		}
	}

	int check(string const &path, bool quiet) {
		error_state err;

		try {
			// FIXME: use quiet flag (pass different reporter)
			err = metadata_check(path);

		} catch (std::exception &e) {

			if (!quiet)
				cerr << e.what() << endl;
			return 1;
		}

		return (err == NO_ERROR) ? 0 : 1;
	}

	void usage(ostream &out, string const &cmd) {
		out << "Usage: " << cmd << " [options] {device|file}" << endl
		    << "Options:" << endl
		    << "  {-q|--quiet}" << endl
		    << "  {-h|--help}" << endl
		    << "  {-V|--version}" << endl
		    << "  {--super-block-only}" << endl;
	}
}

int main(int argc, char **argv)
{
	int c;
	bool quiet = false, superblock_only = false;
	const char shortopts[] = "qhV";
	const struct option longopts[] = {
		{ "quiet", no_argument, NULL, 'q'},
		{ "help", no_argument, NULL, 'h'},
		{ "version", no_argument, NULL, 'V'},
		{ "super-block-only", no_argument, NULL, 1},
		{ NULL, no_argument, NULL, 0 }
	};

	while ((c = getopt_long(argc, argv, shortopts, longopts, NULL)) != -1) {
		switch(c) {
		case 'h':
			usage(cout, basename(argv[0]));
			return 0;

		case 'q':
			quiet = true;
			break;

		case 'V':
			cout << THIN_PROVISIONING_TOOLS_VERSION << endl;
			return 0;

		case 1:
			superblock_only = true;
			break;

		default:
			usage(cerr, basename(argv[0]));
			return 1;
		}
	}

	if (argc == optind) {
		cerr << "No input file provided." << endl;
		usage(cerr, basename(argv[0]));
		exit(1);
	}


	return check(argv[optind], quiet);
}

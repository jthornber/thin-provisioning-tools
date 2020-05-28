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

#include <boost/lexical_cast.hpp>
#include <boost/optional.hpp>

#include "version.h"

#include "base/application.h"
#include "base/error_state.h"
#include "base/file_utils.h"
#include "persistent-data/file_utils.h"
#include "thin-provisioning/commands.h"
#include "thin-provisioning/metadata_checker.h"
#include "thin-provisioning/superblock.h"

using namespace base;
using namespace std;
using namespace persistent_data;
using namespace thin_provisioning;

//----------------------------------------------------------------

namespace {
	struct flags {
		flags()
			: ignore_non_fatal_errors(false),
			  quiet(false),
			  clear_needs_check_flag_on_success(false) {
		}

		check_options check_opts;

		bool ignore_non_fatal_errors;

		bool quiet;
		bool clear_needs_check_flag_on_success;
	};

	void clear_needs_check(string const &path) {
		block_manager::ptr bm = open_bm(path, block_manager::READ_WRITE);

		superblock_detail::superblock sb = read_superblock(bm);
		sb.set_needs_check_flag(false);
		write_superblock(bm, sb);
	}

	// Returns 0 on success, 1 on failure (this gets returned directly
	// by main).
	int check(string const &path, flags fs) {
		bool success = false;

		try {
			if (file_utils::get_file_length(path) < persistent_data::MD_BLOCK_SIZE) {
				cerr << "Metadata device/file too small.  Is this binary metadata?"
				     << endl;
				return 1;
			}

			block_manager::ptr bm = open_bm(path);
			output_options output_opts = !fs.quiet ? OUTPUT_NORMAL : OUTPUT_QUIET;
			error_state err = check_metadata(bm, fs.check_opts, output_opts);

			if (fs.ignore_non_fatal_errors)
				success = (err == FATAL) ? false : true;
			else
				success = (err == NO_ERROR) ? true : false;

			if (success && fs.clear_needs_check_flag_on_success)
				clear_needs_check(path);

		} catch (std::exception &e) {
			if (!fs.quiet)
				cerr << e.what() << endl;

			return 1;
		}

		return !success;
	}
}

//----------------------------------------------------------------

thin_check_cmd::thin_check_cmd()
	: command("thin_check")
{
}

void
thin_check_cmd::usage(std::ostream &out) const
{
	out << "Usage: " << get_name() << " [options] {device|file}" << endl
	    << "Options:" << endl
	    << "  {-q|--quiet}" << endl
	    << "  {-h|--help}" << endl
	    << "  {-V|--version}" << endl
	    << "  {--override-mapping-root}" << endl
	    << "  {--clear-needs-check-flag}" << endl
	    << "  {--ignore-non-fatal-errors}" << endl
	    << "  {--skip-mappings}" << endl
	    << "  {--super-block-only}" << endl;
}

int
thin_check_cmd::run(int argc, char **argv)
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
		{ "clear-needs-check-flag", no_argument, NULL, 4 },
		{ "override-mapping-root", required_argument, NULL, 5},
		{ NULL, no_argument, NULL, 0 }
	};

	while ((c = getopt_long(argc, argv, shortopts, longopts, NULL)) != -1) {
		switch(c) {
		case 'h':
			usage(cout);
			return 0;

		case 'q':
			fs.quiet = true;
			break;

		case 'V':
			cout << THIN_PROVISIONING_TOOLS_VERSION << endl;
			return 0;

		case 1:
			// super-block-only
			fs.check_opts.set_superblock_only();
			break;

		case 2:
			// skip-mappings
			fs.check_opts.set_skip_mappings();
			break;

		case 3:
			// ignore-non-fatal-errors
			fs.ignore_non_fatal_errors = true;
			break;

		case 4:
			// clear needs-check flag
			fs.clear_needs_check_flag_on_success = true;
			break;

		case 5:
			// override-mapping-root
			fs.check_opts.set_override_mapping_root(boost::lexical_cast<uint64_t>(optarg));
			break;

		default:
			usage(cerr);
			return 1;
		}
	}

	if (argc == optind) {
		if (!fs.quiet) {
			cerr << "No input file provided." << endl;
			usage(cerr);
		}

		exit(1);
	}

	return check(argv[optind], fs);
}

//----------------------------------------------------------------

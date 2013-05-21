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

#include "thin-provisioning/metadata.h"
#include "thin-provisioning/metadata_checker.h"
#include "version.h"

using namespace persistent_data;
using namespace std;
using namespace thin_provisioning;

namespace {
	enum error_state {
		NO_ERRORS,
		NON_FATAL,	// eg, lost blocks
		FATAL		// needs fixing before pool can be activated
	};

	int check(string const &path, bool quiet) {
		try {
			optional<error_set::ptr> maybe_errors = metadata_check(path);
			if (maybe_errors) {
				if (!quiet)
					cerr << error_selector(*maybe_errors, 3);
				return 1;
			}
		} catch (std::exception &e) {
			if (!quiet)
				cerr << e.what() << endl;
			return 1;
		}

		return 0;
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

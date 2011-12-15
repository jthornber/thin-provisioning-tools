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

#include "metadata.h"
#include "metadata_checker.h"
#include "version.h"

using namespace persistent_data;
using namespace std;
using namespace thin_provisioning;

namespace {
	int check(string const &path) {
		metadata::ptr md(new metadata(path, metadata::OPEN));

		optional<error_set::ptr> maybe_errors = metadata_check(md);
		if (maybe_errors) {
			cerr << error_selector(*maybe_errors, 3);
			return 1;
		}

		return 0;
	}

	void usage(string const &cmd) {
		cerr << "Usage: " << cmd << " {device|file}" << endl;
		cerr << "Options:" << endl;
                cerr << "  {-h|--help}" << endl;
		cerr << "  {-V|--version}" << endl;
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

	if (argc != 2) {
		usage(basename(argv[0]));
		exit(1);
	}

	return check(argv[1]);
}

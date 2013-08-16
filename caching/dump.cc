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

#include <iostream>
#include <getopt.h>
#include <libgen.h>
#if 0
#include "human_readable_format.h"
#include "cache_metadata_dumper.h"
#include "cache_metadata.h"
#include "xml_format.h"
#include "version.h"

using namespace persistent_data;
using namespace std;
using namespace thin_provisioning;

namespace {
	int dump(string const &path, string const &format, bool repair,
		 block_address metadata_snap = 0) {
		try {
			metadata::ptr md(metadata_snap ?
					 new metadata(path, metadata_snap) :
					 new metadata(path, metadata::OPEN));
			emitter::ptr e;

			if (format == "xml")
				e = create_xml_emitter(cout);
			else if (format == "human_readable")
				e = create_human_readable_emitter(cout);
			else {
				cerr << "unknown format '" << format << "'" << endl;
				exit(1);
			}

			metadata_dump(md, e, repair);

		} catch (std::exception &e) {
			cerr << e.what() << endl;
			return 1;
		}

		return 0;
	}

	void usage(ostream &out, string const &cmd) {
		out << "Usage: " << cmd << " [options] {device|file}" << endl
		    << "Options:" << endl
		    << "  {-h|--help}" << endl
		    << "  {-f|--format} {xml|human_readable}" << endl
		    << "  {-r|--repair}" << endl
		    << "  {-m|--metadata-snap}" << endl
		    << "  {-V|--version}" << endl;
	}
}

int main(int argc, char **argv)
{
	int c;
	bool repair = false;
	const char shortopts[] = "hm:f:rV";
	string format = "xml";
	char *end_ptr;

	const struct option longopts[] = {
		{ "help", no_argument, NULL, 'h'},
		{ "format", required_argument, NULL, 'f' },
		{ "repair", no_argument, NULL, 'r'},
		{ "version", no_argument, NULL, 'V'},
		{ NULL, no_argument, NULL, 0 }
	};

	while ((c = getopt_long(argc, argv, shortopts, longopts, NULL)) != -1) {
		switch(c) {
		case 'h':
			usage(cout, basename(argv[0]));
			return 0;

		case 'f':
			format = optarg;
			break;

		case 'r':
			repair = true;
			break;

		case 'V':
			cout << THIN_PROVISIONING_TOOLS_VERSION << endl;
			return 0;

		default:
			usage(cerr, basename(argv[0]));
			return 1;
		}
	}

	if (argc == optind) {
		cerr << "No input file provided." << endl;
		usage(cerr, basename(argv[0]));
		return 1;
	}

	return dump(argv[optind], format, repair, metadata_snap);
}
#else
int main(int argc, char **argv)
{
	return 1;
}
#endif

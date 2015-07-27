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

using namespace persistent_data;
using namespace std;
using namespace thin_provisioning;

struct flags {
	bool find_metadata_snap;
	bool repair;
};

namespace {
	int dump_(string const &path, ostream &out, string const &format, struct flags &flags,
		  block_address metadata_snap) {
		try {
			metadata::ptr md(new metadata(path, metadata_snap));
			emitter::ptr e;

			if (format == "xml")
				e = create_xml_emitter(out);
			else if (format == "human_readable")
				e = create_human_readable_emitter(out);
			else {
				cerr << "unknown format '" << format << "'" << endl;
				exit(1);
			}

			metadata_dump(md, e, flags.repair);

		} catch (std::exception &e) {
			cerr << e.what() << endl;
			return 1;
		}

		return 0;
	}

	int dump(string const &path, char const *output, string const &format, struct flags &flags,
		 block_address metadata_snap = 0) {
		if (output) {
			ofstream out(output);
			return dump_(path, out, format, flags, metadata_snap);
		} else
			return dump_(path, cout, format, flags, metadata_snap);
	}

	void usage(ostream &out, string const &cmd) {
		out << "Usage: " << cmd << " [options] {device|file}" << endl
		    << "Options:" << endl
		    << "  {-h|--help}" << endl
		    << "  {-f|--format} {xml|human_readable}" << endl
		    << "  {-r|--repair}" << endl
		    << "  {-m|--metadata-snap} [block#]" << endl
		    << "  {-o <xml file>}" << endl
		    << "  {-V|--version}" << endl;
	}
}

int thin_dump_main(int argc, char **argv)
{
	int c;
	char const *output = NULL;
	const char shortopts[] = "hm::o:f:rV";
	char *end_ptr;
	string format = "xml";
	block_address metadata_snap = 0;
	struct flags flags;
	flags.find_metadata_snap = flags.repair = false;

	const struct option longopts[] = {
		{ "help", no_argument, NULL, 'h'},
		{ "metadata-snap", optional_argument, NULL, 'm' },
		{ "output", required_argument, NULL, 'o'},
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
			flags.repair = true;
			break;

		case 'm':
			if (optarg) {
				metadata_snap = strtoull(optarg, &end_ptr, 10);
				if (end_ptr == optarg) {
					cerr << "couldn't parse <metadata_snap>" << endl;
					usage(cerr, basename(argv[0]));
					return 1;
				}
			} else
				flags.find_metadata_snap = true;

			break;

		case 'o':
			output = optarg;
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

	if (flags.find_metadata_snap)
		metadata_snap = find_metadata_snap(argv[optind]);

	return dump(argv[optind], output, format, flags, metadata_snap);
}

base::command thin_provisioning::thin_dump_cmd("thin_dump", thin_dump_main);

//----------------------------------------------------------------

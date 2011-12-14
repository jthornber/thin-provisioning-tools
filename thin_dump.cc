// Copyright (C) 20011 Red Hat, Inc. All rights reserved.
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

#include "human_readable_format.h"
#include "metadata_dumper.h"
#include "metadata.h"
#include "xml_format.h"

using namespace persistent_data;
using namespace std;
using namespace thin_provisioning;

//----------------------------------------------------------------

namespace {
	void dump(string const &path, string const &format) {
		metadata::ptr md(new metadata(path, metadata::OPEN));
		emitter::ptr e;

		if (format == "xml")
			e = create_xml_emitter(cout);
		else if (format == "human_readable")
			e = create_human_readable_emitter(cout);
		else {
			cerr << "unknown format '" << format << "'" << endl;
			exit(1);
		}

		metadata_dump(md, e);
	}

	void usage(void) {
		cerr << "Usage: thin_dump [options] <metadata device or file>" << endl << endl;
		cerr << "Options:" << endl;
                cerr << "  -h [ --help ]              Produce help message" << endl;
  		cerr << "  -f [ --format ] arg (=xml) Select format (human_readable|xml)" << endl;
		cerr << "  -i [ --input ] arg         Input file" << endl;
	}
}

int main(int argc, char **argv)
{
	int c;
	const char shortopts[] = "hf:i:";
	string filename, format = "xml";
	const struct option longopts[] = {
		{ "help", no_argument, NULL, 'h'},
		{ "format", required_argument, NULL, 'f' },
		{ "input", required_argument, NULL, 'i'},
		{ NULL, no_argument, NULL, 0 }
	};

	while ((c = getopt_long(argc, argv, shortopts, longopts, NULL)) != -1) {
		switch(c) {
			case 'h':
				usage();
				return 1;
			case 'f':
				format = optarg;
				break;
			case 'i':
				filename = optarg;
				break;
		}
	}

	if (argc == 1 ||
	    filename.empty()) {
		usage();
		return 1;
	}

	dump(filename, format);
	return 0;
}

//----------------------------------------------------------------

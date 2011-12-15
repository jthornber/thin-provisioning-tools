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

#include "emitter.h"
#include "human_readable_format.h"
#include "metadata.h"
#include "restore_emitter.h"
#include "xml_format.h"
#include "version.h"

#include <fstream>
#include <iostream>
#include <getopt.h>
#include <libgen.h>

using namespace persistent_data;
using namespace std;
using namespace thin_provisioning;

//----------------------------------------------------------------

namespace {
	void restore(string const &backup_file, string const &dev) {
		// FIXME: hard coded
		block_address const NR_BLOCKS = 100000;

		metadata::ptr md(new metadata(dev, metadata::CREATE, 128, NR_BLOCKS));
		emitter::ptr restorer = create_restore_emitter(md);
		ifstream in(backup_file.c_str(), ifstream::in);
		// FIXME: 
		//try {
			parse_xml(in, restorer);
#if 0
		} catch (...) {
			in.close();
			throw;
		}
#endif
	}

	void usage(string const &cmd) {
		cerr << "Usage: " << cmd << " [options]" << endl << endl;
		cerr << "Options:" << endl;
		cerr << "  {-h|--help}" << endl;
		cerr << "  {-i|--input} input_file" << endl;
		cerr << "  {-o [ --output} {device|file}" << endl;
		cerr << "  {-V|--version}" << endl;
	}
}

int main(int argc, char **argv)
{
	int c;
	const char *shortopts = "hi:o:V";
	string input, output;
	const struct option longopts[] = {
		{ "help", no_argument, NULL, 'h'},
		{ "input", required_argument, NULL, 'i' },
		{ "output", required_argument, NULL, 'o'},
		{ "version", no_argument, NULL, 'V'},
		{ NULL, no_argument, NULL, 0 }
	};

	while ((c = getopt_long(argc, argv, shortopts, longopts, NULL)) != -1) {
		switch(c) {
			case 'h':
				usage(basename(argv[0]));
				return 0;
			case 'i':
				input = optarg;
				break;
			case 'o':
				output = optarg;
				break;
			case 'V':
				cerr << THIN_PROVISIONING_TOOLS_VERSION << endl;
				return 0;
		}
	}

	if (argc == 1) {
		usage(basename(argv[0]));
		return 1;
	}

        if (input.empty()) {
		cerr << "No input file provided." << endl;
		return 1;
	}

	if (output.empty()) {
		cerr << "No output file provided." << endl;
		return 1;
	}

	restore(input, output);
	return 0;

}

//----------------------------------------------------------------

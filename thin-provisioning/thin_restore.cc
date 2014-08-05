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

#include "persistent-data/file_utils.h"
#include "thin-provisioning/emitter.h"
#include "thin-provisioning/human_readable_format.h"
#include "thin-provisioning/metadata.h"
#include "thin-provisioning/restore_emitter.h"
#include "thin-provisioning/xml_format.h"
#include "version.h"

#include <fstream>
#include <getopt.h>
#include <iostream>
#include <libgen.h>
#include <linux/fs.h>
#include <sys/ioctl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

using namespace persistent_data;
using namespace std;
using namespace thin_provisioning;

//----------------------------------------------------------------

namespace {
	size_t get_file_length(string const &file) {
		struct stat info;
		int r;

		r = ::stat(file.c_str(), &info);
		if (r)
			throw runtime_error("Couldn't stat backup path");

		return info.st_size;
	}

	progress_monitor::ptr create_monitor(bool quiet) {
		if (!quiet && isatty(fileno(stdout)))
			return create_progress_bar("Restoring");
		else
			return create_quiet_progress_monitor();
	}

	int restore(string const &backup_file, string const &dev, bool quiet) {
		try {
			// The block size gets updated by the restorer.
			metadata::ptr md(new metadata(dev, metadata::CREATE, 128, 0));
			emitter::ptr restorer = create_restore_emitter(md);

			check_file_exists(backup_file);
			ifstream in(backup_file.c_str(), ifstream::in);

			progress_monitor::ptr monitor = create_monitor(quiet);
			parse_xml(in, restorer, get_file_length(backup_file), monitor);

		} catch (std::exception &e) {
			cerr << e.what() << endl;
			return 1;
		}

		return 0;
	}

	void usage(ostream &out, string const &cmd) {
		out << "Usage: " << cmd << " [options]" << endl
		    << "Options:" << endl
		    << "  {-h|--help}" << endl
		    << "  {-i|--input} <input xml file>" << endl
		    << "  {-o|--output} <output device or file>" << endl
		    << "  {-q|--quiet}" << endl
		    << "  {-V|--version}" << endl;
	}
}

int main(int argc, char **argv)
{
	int c;
	char const *prog_name = basename(argv[0]);
	const char *shortopts = "hi:o:qV";
	string input, output;
	bool quiet = false;
	const struct option longopts[] = {
		{ "help", no_argument, NULL, 'h'},
		{ "input", required_argument, NULL, 'i' },
		{ "output", required_argument, NULL, 'o'},
		{ "quiet", no_argument, NULL, 'q'},
		{ "version", no_argument, NULL, 'V'},
		{ NULL, no_argument, NULL, 0 }
	};

	while ((c = getopt_long(argc, argv, shortopts, longopts, NULL)) != -1) {
		switch(c) {
		case 'h':
			usage(cout, prog_name);
			return 0;

		case 'i':
			input = optarg;
			break;

		case 'o':
			output = optarg;
			break;

		case 'q':
			quiet = true;
			break;

		case 'V':
			cout << THIN_PROVISIONING_TOOLS_VERSION << endl;
			return 0;

		default:
			usage(cerr, prog_name);
			return 1;
		}
	}

	if (argc != optind) {
		usage(cerr, prog_name);
		return 1;
	}

        if (input.empty()) {
		cerr << "No input file provided." << endl << endl;
		usage(cerr, prog_name);
		return 1;
	}

	if (output.empty()) {
		cerr << "No output file provided." << endl << endl;
		usage(cerr, prog_name);
		return 1;
	}

	return restore(input, output, quiet);
}

//----------------------------------------------------------------

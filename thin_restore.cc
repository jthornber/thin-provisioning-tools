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

#include <fstream>
#include <iostream>
#include <boost/program_options.hpp>

using namespace persistent_data;
using namespace std;
using namespace thin_provisioning;

namespace po = boost::program_options;

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

	void usage(po::options_description const &desc) {
		cerr << "Usage: thin_restore [options]" << endl << endl;
		cerr << desc;
	}
}

int main(int argc, char **argv)
{
	po::options_description desc("Options");
	desc.add_options()
		("help", "Produce help message")
		("input,i", po::value<string>(), "Input file")
		("output,o", po::value<string>(), "Output file")
		;

	po::variables_map vm;
	po::store(po::command_line_parser(argc, argv).options(desc).run(), vm);
	po::notify(vm);

	if (vm.count("help")) {
		usage(desc);
		return 0;
	}

	if (vm.count("input") != 1) {
		cerr << "No input file provided." << endl;
		usage(desc);
		return 1;
	}

	if (vm.count("output") != 1) {
		cerr << "No output file provided." << endl;
		usage(desc);
		return 1;
	}

	restore(vm["input"].as<string>(),
		vm["output"].as<string>());

	return 0;

}

//----------------------------------------------------------------

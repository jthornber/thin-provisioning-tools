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
		metadata::ptr md(new metadata(dev, metadata::CREATE));
		emitter::ptr restorer = create_restore_emitter(md);
		ifstream in(backup_file.c_str(), ifstream::in);
		try {
			parse_xml(in, restorer);

		} catch (...) {
			in.close();
			throw;
		}
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

#include <iostream>

#include "human_readable_format.h"
#include "metadata_dumper.h"
#include "metadata_ll.h"
#include "xml_format.h"

#include <boost/program_options.hpp>

using namespace persistent_data;
using namespace std;
using namespace thin_provisioning;

namespace po = boost::program_options;

//----------------------------------------------------------------

namespace {
	void dump(string const &path, string const &format) {
		metadata_ll::ptr md(new metadata_ll(path));
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

	void usage(po::options_description const &desc) {
		cerr << "Usage: thin_dump [options] <metadata device or file>" << endl << endl;
		cerr << desc;
	}
}

int main(int argc, char **argv)
{
	po::options_description desc("Options");
	desc.add_options()
		("help", "Produce help message")
		("format,f", po::value<string>()->default_value("xml"), "Select format (human_readable|xml)")
		("input,i", po::value<string>(), "Input file")
		;

	po::positional_options_description p;
	p.add("input", -1);

	po::variables_map vm;
	po::store(po::command_line_parser(argc, argv).options(desc).positional(p).run(), vm);
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

	dump(vm["input"].as<string>(), vm["format"].as<string>());

	return 0;
}

//----------------------------------------------------------------

#include <fstream>
#include <getopt.h>
#include <libgen.h>
#include <iostream>

#include "version.h"
#include "caching/mapping_array.h"
#include "caching/metadata.h"
#include "caching/xml_format.h"
#include "persistent-data/file_utils.h"

using namespace std;
using namespace caching;
using namespace caching::mapping_array_detail;
using namespace caching::superblock_detail;

//----------------------------------------------------------------

namespace {
	string to_string(unsigned char const *data) {
		// FIXME: we're assuming the data is zero terminated here
		return std::string(reinterpret_cast<char const *>(data));
	}

	//--------------------------------

	string const STDOUT_PATH("-");

	bool want_stdout(string const &output) {
		return output == STDOUT_PATH;
	}

	int dump_(string const &dev, ostream &out) {
		block_manager<>::ptr bm = open_bm(dev, block_io<>::READ_ONLY);
		metadata::ptr md(new metadata(bm, metadata::OPEN));
		emitter::ptr e = create_xml_emitter(out);

		superblock const &sb = md->sb_;
		e->begin_superblock(to_string(sb.uuid), sb.data_block_size,
				    sb.cache_blocks, to_string(sb.policy_name),
				    sb.policy_hint_size);

		e->begin_mappings();

		for (unsigned cblock = 0; cblock < sb.cache_blocks; cblock++) {
			mapping m = md->mappings_->get(cblock);

			if (m.flags_ & M_VALID)
				e->mapping(cblock, m.oblock_, m.flags_ & M_DIRTY);
		}

		e->end_mappings();

		e->end_superblock();

		return 0;
	}

	int dump(string const &dev, string const &output) {
		try {
			if (want_stdout(output))
				return dump_(dev, cout);
			else {
				ofstream out(output.c_str());
				return dump_(dev, out);
			}

		} catch (std::exception &e) {
			cerr << e.what() << endl;
			return 1;
		}
	}

	void usage(ostream &out, string const &cmd) {
		out << "Usage: " << cmd << " [options] {device|file}" << endl
		    << "Options:" << endl
		    << "  {-h|--help}" << endl
		    << "  {-o <xml file>}" << endl
		    << "  {-V|--version}" << endl;
	}
}

//----------------------------------------------------------------

int main(int argc, char **argv)
{
	int c;
	string output("-");
	char const shortopts[] = "ho:V";

	option const longopts[] = {
		{ "help", no_argument, NULL, 'h'},
		{ "output", required_argument, NULL, 'o'},
		{ "version", no_argument, NULL, 'V'},
		{ NULL, no_argument, NULL, 0 }
	};

	while ((c = getopt_long(argc, argv, shortopts, longopts, NULL)) != -1) {
		switch(c) {
		case 'h':
			usage(cout, basename(argv[0]));
			return 0;

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

	return dump(argv[optind], output);
}

//----------------------------------------------------------------

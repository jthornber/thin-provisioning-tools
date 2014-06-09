#include <boost/lexical_cast.hpp>
#include <boost/optional.hpp>
#include <getopt.h>
#include <iostream>
#include <libgen.h>

#include "version.h"

using namespace std;

//----------------------------------------------------------------

namespace {
	class application {
	public:
		application(string const &cmd)
		: cmd_(cmd) {
		}

		void usage(ostream &out) {
			out << "Usage: " << cmd_ << " [options] --snap1 <snap> --snap2 <snap> <device or file>" << endl
			    << "Options:" << endl
			    << "  {-h|--help}" << endl
			    << "  {-V|--version}" << endl;
		}

		void die(string const &msg) {
			cerr << msg << endl;
			usage(cerr);
			exit(1);
		}

		unsigned parse_snap(string const &str) {
			try {
				return boost::lexical_cast<unsigned>(str);

			} catch (...) {
				ostringstream out;
				out << "Couldn't parse snapshot designator: '" << str << "'";
				die(out.str());
			}

			return 0; // never get here
		}

	private:
		string cmd_;
	};

	struct flags {
		boost::optional<string> dev;
		boost::optional<unsigned> snap1;
		boost::optional<unsigned> snap2;
	};
}

//----------------------------------------------------------------

// FIXME: add metadata snap switch

int main(int argc, char **argv)
{
	int c;
	flags fs;
	application app(basename(argv[0]));

	char const shortopts[] = "hV";
	option const longopts[] = {
		{ "help", no_argument, NULL, 'h' },
		{ "version", no_argument, NULL, 'V' },
		{ "snap1", required_argument, NULL, 1 },
		{ "snap2", required_argument, NULL, 2 },
	};

	while ((c = getopt_long(argc, argv, shortopts, longopts, NULL)) != -1) {
		switch (c) {
		case 'h':
			app.usage(cout);
			return 0;

		case 'V':
			cout << THIN_PROVISIONING_TOOLS_VERSION << endl;
			return 0;

		case 1:
			fs.snap1 = app.parse_snap(optarg);
			break;

		case 2:
			fs.snap2 = app.parse_snap(optarg);
			break;

		default:
			app.usage(cerr);
			return 1;
		}
	}

	if (argc == optind)
		app.die("No input device provided.");
	else
		fs.dev = argv[optind];

	if (!fs.snap1)
		app.die("--snap1 not specified.");

	if (!fs.snap2)
		app.die("--snap2 not specified.");

	return 0;
}

//----------------------------------------------------------------

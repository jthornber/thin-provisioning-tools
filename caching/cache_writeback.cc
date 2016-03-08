#ifndef CACHING_CACHE_WRITEBACK_H
#define CACHING_CACHE_WRITEBACK_H

#include "caching/commands.h"
#include "version.h"

#include <boost/optional.hpp>
#include <getopt.h>
#include <string>

using namespace caching;
using namespace boost;
using namespace std;

//----------------------------------------------------------------

namespace {
	struct flags {
		using maybe_string = boost::optional<string>;

		maybe_string metadata_dev;
		maybe_string origin_dev;
		maybe_string fast_dev;
	};

	int writeback(flags const &f) {
		return 1;
	}
}

//----------------------------------------------------------------

cache_writeback_cmd::cache_writeback_cmd()
	: command("cache_writeback")
{
}

void
cache_writeback_cmd::usage(std::ostream &out) const
{
	out << "Usage: " << get_name() << " [options]\n"
	    << "\t\t--metadata-device <dev>\n"
	    << "\t\t--origin-device <dev>\n"
	    << "\t\t--fast-device <dev>\n"
	    << "Options:\n"
	    << "  {-h|--help}\n"
	    << "  {-V|--version}" << endl;
}

int
cache_writeback_cmd::run(int argc, char **argv)
{
	int c;
	flags fs;
	char const *short_opts = "hV";
	option const long_opts[] = {
		{ "metadata-device", required_argument, NULL, 0 },
		{ "origin-device", required_argument, NULL, 1 },
		{ "fast-device", required_argument, NULL, 2 },
		{ "help", no_argument, NULL, 'h'},
		{ "version", no_argument, NULL, 'V'},
		{ NULL, no_argument, NULL, 0 }
	};

	while ((c = getopt_long(argc, argv, short_opts, long_opts, NULL)) != -1) {
		switch(c) {
		case 0:
			fs.metadata_dev = optarg;
			break;

		case 1:
			fs.origin_dev = optarg;
			break;

		case 2:
			fs.fast_dev = optarg;
			break;

		case 'h':
			usage(cout);
			return 0;

		case 'V':
			cout << THIN_PROVISIONING_TOOLS_VERSION << endl;
			return 0;

		default:
			usage(cerr);
			return 1;
		}
	}

	if (argc != optind) {
		usage(cerr);
		return 1;
	}

        if (!fs.metadata_dev) {
		cerr << "No metadata device provided.\n\n";
		usage(cerr);
		return 1;
	}

	if (!fs.origin_dev) {
		cerr << "No origin device provided.\n\n";
		usage(cerr);
		return 1;
	}

	if (!fs.fast_dev) {
		cerr << "No fast device provided.\n\n";
		usage(cerr);
		return 1;
	}

	return writeback(fs);

}

//----------------------------------------------------------------

#endif

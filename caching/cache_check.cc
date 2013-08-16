#include <iostream>
#include <sstream>
#include <string>
#include <stdexcept>

#include <errno.h>
#include <fcntl.h>
#include <getopt.h>
#include <libgen.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include "base/error_state.h"
#include "base/nested_output.h"

using namespace std;

//----------------------------------------------------------------

namespace {
	struct stat guarded_stat(string const &path) {
		struct stat info;

		int r = ::stat(path.c_str(), &info);
		if (r) {
			ostringstream msg;
			char buffer[128], *ptr;

			ptr = ::strerror_r(errno, buffer, sizeof(buffer));
			msg << path << ": " << ptr;
			throw runtime_error(msg.str());
		}

		return info;
	}

	int open_file(string const &path, int mode) {
		int fd = open(path.c_str(), mode);
		if (fd < 0) {
			ostringstream msg;
			char buffer[128], *ptr;

			ptr = strerror_r(errno, buffer, sizeof(buffer));
			msg << path << ": " << ptr;
			throw runtime_error(msg.str());
		}

		return fd;
	}

	int check(string const &path, bool quiet) {
		struct stat info = guarded_stat(path);

		if (!S_ISREG(info.st_mode) && !S_ISBLK(info.st_mode)) {
			ostringstream msg;
			msg << path << ": " << "Not a block device or regular file";
			throw runtime_error(msg.str());
		}

		int fd = open_file(path, O_RDONLY);

		ostringstream msg;
		msg << path << ": " << "No superblock found";
		throw runtime_error(msg.str());
		return 0;

#if 0
		try {
			metadata::ptr md(new metadata(path, metadata::OPEN));

			optional<error_set::ptr> maybe_errors = metadata_check(md);
			if (maybe_errors) {
				if (!quiet)
					cerr << error_selector(*maybe_errors, 3);
				return 1;
			}
		} catch (std::exception &e) {
			if (!quiet)
				cerr << e.what() << endl;
			return 1;
		}

		return 0;
#endif
	}

	void usage(ostream &out, string const &cmd) {
		out << "Usage: " << cmd << " [options] {device|file}" << endl
		    << "Options:" << endl
		    << "  {-q|--quiet}" << endl
		    << "  {-h|--help}" << endl
		    << "  {-V|--version}" << endl;
	}

	char const *TOOLS_VERSION = "0.1.6";
}

//----------------------------------------------------------------

int main(int argc, char **argv)
{
	int c;
	bool quiet = false;
	const char shortopts[] = "qhV";
	const struct option longopts[] = {
		{ "quiet", no_argument, NULL, 'q'},
		{ "help", no_argument, NULL, 'h'},
		{ "version", no_argument, NULL, 'V'},
		{ NULL, no_argument, NULL, 0 }
	};

	while ((c = getopt_long(argc, argv, shortopts, longopts, NULL)) != -1) {
		switch(c) {
		case 'h':
			usage(cout, basename(argv[0]));
			return 0;

		case 'q':
			quiet = true;
			break;

		case 'V':
			cout << TOOLS_VERSION << endl;
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

	try {
		check(argv[optind], quiet);

	} catch (exception const &e) {
		cerr << e.what() << endl;
		return 1;
	}

	return 0;
}

//----------------------------------------------------------------

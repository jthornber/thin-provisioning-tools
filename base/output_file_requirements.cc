#include "base/output_file_requirements.h"

#include <iostream>
#include <linux/fs.h>
#include <sstream>
#include <stdexcept>
#include <sys/ioctl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

using namespace base;
using namespace std;

//----------------------------------------------------------------

namespace {
	void explain_output_file_requirements() {
		ostringstream out;
		out << "The output file should either be a block device,\n"
		    << "or an existing file.  The file needs to be large\n"
		    << "enough to hold the metadata.";

		throw runtime_error(out.str());
	}

	unsigned const MIN_SIZE = 32 * 1024;
}

void
base::check_output_file_requirements(string const &path)
{
	struct stat info;
	int r = ::stat(path.c_str(), &info);
	if (r) {
		cerr << "Output file does not exist.\n\n";
		explain_output_file_requirements();
	}

	if (!info.st_size) {
		cerr << "Zero size output file.\n\n";
		explain_output_file_requirements();
	}

	if (info.st_size < MIN_SIZE) {
		cerr << "Output file too small.\n\n";
		explain_output_file_requirements();
	}
}

//----------------------------------------------------------------

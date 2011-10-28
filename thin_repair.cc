#include <iostream>

#include "metadata.h"
#include "metadata_checker.h"

using namespace boost;
using namespace persistent_data;
using namespace std;
using namespace thin_provisioning;

namespace {
	int check(string const &path) {
		metadata::ptr md(new metadata(path));

		optional<error_set::ptr> maybe_errors = metadata_check(md);
		if (maybe_errors) {
			cerr << error_selector(*maybe_errors, 3);
			return 1;
		}

		return 0;
	}

	void usage(string const &cmd) {
		cerr << "Usage: " << cmd << " <metadata device>" << endl;
	}
}

int main(int argc, char **argv)
{
	if (argc != 2) {
		usage(argv[0]);
		exit(1);
	}

	return check(argv[1]);
}

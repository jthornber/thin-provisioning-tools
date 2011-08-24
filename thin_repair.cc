#include <iostream>

#include "metadata.h"

using namespace persistent_data;
using namespace std;
using namespace thin_provisioning;

namespace {
	int check(string const &path) {
		metadata md(path);

		auto maybe_errors = md.check();
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

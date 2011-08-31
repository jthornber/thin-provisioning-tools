#include <iostream>

#include "metadata.h"

using namespace persistent_data;
using namespace std;
using namespace thin_provisioning;

//----------------------------------------------------------------

namespace {
	void dump(string const &path) {
		metadata md(path);
		human_readable::ptr emitter(new human_readable);

		md.dump();
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

	dump(argv[1]);

	return 0;
}

//----------------------------------------------------------------

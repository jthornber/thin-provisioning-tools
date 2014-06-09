#include <getopt.h>
#include <iostream>

#include "version.h"

using namespace std;

//----------------------------------------------------------------

int main(int argc, char **argv)
{
	int c;
	char const shortopts[] = "V";
	option const longopts[] = {
		{ "version", no_argument, NULL, 'V'}
	};

	while ((c = getopt_long(argc, argv, shortopts, longopts, NULL)) != -1) {
		switch (c) {
		case 'V':
			cout << THIN_PROVISIONING_TOOLS_VERSION << endl;
			return 0;
		}
	}

	return 0;
}

//----------------------------------------------------------------

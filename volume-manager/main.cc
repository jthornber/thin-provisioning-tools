#include "device-mapper/ioctl_interface.h"

#include <iostream>

using namespace dm;
using namespace std;

//----------------------------------------------------------------

int main(int argc, char **argv)
{
	ioctl_interface dm;

	version_instr v;
	dm.execute(v);
	cout << "dm version: "
	     << v.get_version()[0] << ", "
	     << v.get_version()[1] << ", "
	     << v.get_version()[2] << "\n";

	return 0;
}

//----------------------------------------------------------------


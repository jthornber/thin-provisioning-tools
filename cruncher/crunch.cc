#include "cruncher/commands.h"
#include <iostream>

using namespace std;

//----------------------------------------------------------------

int crunch_main(int argc, char **argv)
{
	cerr << "Hello, world!\n";

	return 0;
}

base::command cruncher::crunch_cmd("crunch", crunch_main);

//----------------------------------------------------------------

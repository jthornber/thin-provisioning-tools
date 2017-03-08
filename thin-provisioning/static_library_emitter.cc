#include "thin-provisioning/shared_library_emitter.h"
#include <stdexcept>
#include "contrib/tmakatos_emitter.h"
#include <iostream>

using namespace std;
using namespace thin_provisioning;

//----------------------------------------------------------------

emitter::ptr
thin_provisioning::create_custom_emitter(string const &shared_lib, ostream &out)
{
	if (shared_lib != "tmakatos_emitter.so")
		throw runtime_error(shared_lib + ": no such emitter");

	return emitter::ptr(new tmakatos_emitter::binary_emitter(out));
}

//----------------------------------------------------------------

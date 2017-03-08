#include "thin-provisioning/shared_library_emitter.h"

#include <dlfcn.h>
#include <stdexcept>

using namespace std;
using namespace thin_provisioning;

//----------------------------------------------------------------

emitter::ptr
thin_provisioning::create_custom_emitter(string const &shared_lib, ostream &out)
{
	emitter::ptr (*create_fn)(ostream &out);
	void *handle = dlopen(shared_lib.c_str(), RTLD_LAZY);
	if (!handle)
		throw runtime_error(dlerror());

	dlerror();    // Clear any existing error
	create_fn = reinterpret_cast<emitter::ptr (*)(ostream &)>(dlsym(handle, "create_emitter"));

	char *error = dlerror();
	if (error)
		throw runtime_error(error);

	return create_fn(out);
}

//----------------------------------------------------------------

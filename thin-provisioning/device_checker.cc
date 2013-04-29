#include "thin-provisioning/device_checker.h"

using namespace thin_provisioning;

//----------------------------------------------------------------

device_checker::device_checker(block_manager::ptr bm)
	: checker(bm)
{
}

damage_list_ptr
device_checker::check()
{
	// FIXME: finish.
	return damage_list_ptr(new damage_list);
}

//----------------------------------------------------------------

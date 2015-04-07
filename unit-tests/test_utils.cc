#include "test_utils.h"

#include "persistent-data/space-maps/core.h"

using namespace persistent_data;

//----------------------------------------------------------------

void test::zero_block(block_manager<>::ptr bm, block_address b)
{
	block_manager<>::write_ref wr = bm->write_lock(b);
	memset(wr.data(), 0, 4096);
}

transaction_manager::ptr
test::open_temporary_tm(block_manager<>::ptr bm)
{
	space_map::ptr sm(new core_map(bm->get_nr_blocks()));
	transaction_manager::ptr tm(new transaction_manager(bm, sm));
	return tm;
}

//----------------------------------------------------------------

#include "test_utils.h"

//----------------------------------------------------------------

void test::zero_block(block_manager<>::ptr bm, block_address b)
{
	block_manager<>::write_ref wr = bm->write_lock(b);
	memset(&wr.data(), 0, sizeof(wr.data()));
}

//----------------------------------------------------------------

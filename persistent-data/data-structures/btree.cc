#include "persistent-data/data-structures/btree.h"

using namespace persistent_data;

//----------------------------------------------------------------

block_ref_counter::block_ref_counter(space_map::ptr sm)
	: sm_(sm) {
}

void
block_ref_counter::set(block_address const &b, uint32_t rc) {
	sm_->set_count(b, rc);
}

void
block_ref_counter::inc(block_address const &b) {
	sm_->inc(b);
}

void
block_ref_counter::dec(block_address const &b) {
	sm_->dec(b);
}

//----------------------------------------------------------------



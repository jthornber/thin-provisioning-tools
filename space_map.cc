#include "space_map.h"

using namespace persistent_data;

//----------------------------------------------------------------

sm_decrementer::sm_decrementer(space_map::ptr sm, block_address b)
	: sm_(sm),
	  b_(b),
	  released_(false) {
}

sm_decrementer::~sm_decrementer() {
	if (!released_)
		sm_->dec(b_);
}

void
sm_decrementer::dont_bother() {
	released_ = true;
}

//----------------------------------------------------------------

#include "persistent-data/data-structures/btree_damage_visitor.h"

using namespace persistent_data;

//----------------------------------------------------------------

damage_tracker::damage_tracker()
	: damaged_(false),
	  damage_begin_(0)
{
}

void
damage_tracker::bad_node()
{
	damaged_ = true;
}

maybe_range64
damage_tracker::good_internal(block_address begin)
{
	maybe_range64 r;

	if (damaged_) {
		r = maybe_range64(range64(damage_begin_, begin));
		damaged_ = false;
	}

	damage_begin_ = begin;
	return r;
}

maybe_range64
damage_tracker::good_leaf(uint64_t begin, uint64_t end)
{
	maybe_range64 r;

	if (damaged_) {
		r = maybe_range64(range64(damage_begin_, begin));
		damaged_ = false;
	}

	damage_begin_ = end;
	return r;
}

maybe_range64
damage_tracker::end()
{
	if (damaged_)
		return maybe_range64(damage_begin_);
	else
		return maybe_range64();
}

//----------------------------------------------------------------

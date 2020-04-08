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

damage_tracker::maybe_run64
damage_tracker::good_internal(block_address begin)
{
	maybe_run64 r;

	if (damaged_) {
		r = maybe_run64(run64(damage_begin_, begin));
		damaged_ = false;
	}

	damage_begin_ = begin;
	return r;
}

damage_tracker::maybe_run64
damage_tracker::good_leaf(block_address begin, block_address end)
{
	maybe_run64 r;

	if (damaged_) {
		r = maybe_run64(run64(damage_begin_, begin));
		damaged_ = false;
	}

	damage_begin_ = end;
	return r;
}

damage_tracker::maybe_run64
damage_tracker::end()
{
	maybe_run64 r;

	if (damaged_)
		r = maybe_run64(damage_begin_);
	else
		r = maybe_run64();

	damaged_ = false;
	damage_begin_ = 0;

	return r;
}

//----------------------------------------------------------------

path_tracker::path_tracker()
{
	// We push an empty path, to ensure there
	// is always a current_path.
	paths_.push_back(btree_path());
}

btree_path const *
path_tracker::next_path(btree_path const &p)
{
	if (p != current_path()) {
		if (paths_.size() == 2)
			paths_.pop_front();
		paths_.push_back(p);

		return &paths_.front();
	}

	return NULL;
}

btree_path const &
path_tracker::current_path() const
{
	return paths_.back();
}

//----------------------------------------------------------------

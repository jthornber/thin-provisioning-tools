#include "transaction_manager.h"

#include <string.h>

using namespace boost;
using namespace persistent_data;
using namespace std;

//----------------------------------------------------------------

transaction_manager::transaction_manager(block_manager<>::ptr bm,
					 space_map::ptr sm)
	: bm_(bm),
	  sm_(sm)
{
}

transaction_manager::~transaction_manager()
{
}

transaction_manager::write_ref
transaction_manager::begin(block_address superblock)
{
	write_ref wr = bm_->superblock(superblock);
	wipe_shadow_table();
	return wr;
}

transaction_manager::write_ref
transaction_manager::begin(block_address superblock, validator v)
{
	write_ref wr = bm_->superblock(superblock, v);
	wipe_shadow_table();
	return wr;
}

// FIXME: these explicit try/catches are gross
transaction_manager::write_ref
transaction_manager::new_block()
{
	block_address b = sm_->new_block();
	try {
		add_shadow(b);
		try {
			return bm_->write_lock_zero(b);
		} catch (...) {
			remove_shadow(b);
			throw;
		}

	} catch (...) {
		sm_->dec(b);
		throw;
	}
}

transaction_manager::write_ref
transaction_manager::new_block(validator v)
{
	block_address b = sm_->new_block();
	try {
		add_shadow(b);
		try {
			return bm_->write_lock_zero(b, v);
		} catch (...) {
			remove_shadow(b);
			throw;
		}
	} catch (...) {
		sm_->dec(b);
		throw;
	}
}

// FIXME: make exception safe
pair<transaction_manager::write_ref, bool>
transaction_manager::shadow(block_address orig)
{
	if (is_shadow(orig) &&
	    !sm_->count_possibly_greater_than_one(orig))
		return make_pair(bm_->write_lock(orig), false);

	read_ref src = bm_->read_lock(orig);
	write_ref dest = bm_->write_lock_zero(sm_->new_block());
	::memcpy(dest.data(), src.data(), MD_BLOCK_SIZE);

	ref_t count = sm_->get_count(orig);
	if (count == 0)
		throw runtime_error("shadowing free block");
	sm_->dec(orig);
	add_shadow(dest.get_location());
	return make_pair(dest, count > 1);
}

// FIXME: duplicate code
pair<transaction_manager::write_ref, bool>
transaction_manager::shadow(block_address orig, validator v)
{
	if (is_shadow(orig) &&
	    sm_->count_possibly_greater_than_one(orig))
		return make_pair(bm_->write_lock(orig), false);

	read_ref src = bm_->read_lock(orig, v);
	write_ref dest = bm_->write_lock_zero(sm_->new_block(), v);
	::memcpy(dest.data(), src.data(), MD_BLOCK_SIZE);

	ref_t count = sm_->get_count(orig);
	if (count == 0)
		throw runtime_error("shadowing free block");
	sm_->dec(orig);
	add_shadow(dest.get_location());
	return make_pair(dest, count > 1);
}

transaction_manager::read_ref
transaction_manager::read_lock(block_address b)
{
	return bm_->read_lock(b);
}

transaction_manager::read_ref
transaction_manager::read_lock(block_address b, validator v)
{
	return bm_->read_lock(b, v);
}

void
transaction_manager::add_shadow(block_address b)
{
	shadows_.insert(b);
}

void
transaction_manager::remove_shadow(block_address b)
{
	shadows_.erase(b);
}

bool
transaction_manager::is_shadow(block_address b) const
{
	return shadows_.count(b) > 0;
}

void
transaction_manager::wipe_shadow_table()
{
	shadows_.clear();
}

//----------------------------------------------------------------

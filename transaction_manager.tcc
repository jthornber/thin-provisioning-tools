#include "transaction_manager.h"

#include <string.h>

using namespace boost;
using namespace persistent_data;
using namespace std;

//----------------------------------------------------------------

template <uint32_t BlockSize>
transaction_manager<BlockSize>::transaction_manager(typename block_manager<BlockSize>::ptr bm,
						    space_map::ptr sm)
	: bm_(bm),
	  sm_(sm)
{
}

template <uint32_t BlockSize>
transaction_manager<BlockSize>::~transaction_manager()
{
}

template <uint32_t BlockSize>
typename transaction_manager<BlockSize>::write_ref
transaction_manager<BlockSize>::begin(block_address superblock)
{
	write_ref wr = bm_->superblock(superblock);
	wipe_shadow_table();
	return wr;
}

template <uint32_t BlockSize>
typename transaction_manager<BlockSize>::write_ref
transaction_manager<BlockSize>::begin(block_address superblock,
				      validator v)
{
	write_ref wr = bm_->superblock(superblock, v);
	wipe_shadow_table();
	return wr;
}

// FIXME: these explicit try/catches are gross
template <uint32_t BlockSize>
typename transaction_manager<BlockSize>::write_ref
transaction_manager<BlockSize>::new_block()
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

template <uint32_t BlockSize>
typename transaction_manager<BlockSize>::write_ref
transaction_manager<BlockSize>::new_block(validator v)
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
template <uint32_t BlockSize>
pair<typename transaction_manager<BlockSize>::write_ref, bool>
transaction_manager<BlockSize>::shadow(block_address orig)
{
	if (is_shadow(orig) &&
	    !sm_->count_possibly_greater_than_one(orig))
		return make_pair(bm_->write_lock(orig), false);

	read_ref src = bm_->read_lock(orig);
	write_ref dest = bm_->write_lock_zero(sm_->new_block());
	::memcpy(dest.data(), src.data(), BlockSize);

	ref_t count = sm_->get_count(orig);
	if (count == 0)
		throw runtime_error("shadowing free block");
	sm_->dec(orig);
	add_shadow(dest.get_location());
	return make_pair(dest, count > 1);
}

// FIXME: duplicate code
template <uint32_t BlockSize>
pair<typename transaction_manager<BlockSize>::write_ref, bool>
transaction_manager<BlockSize>::shadow(block_address orig, validator v)
{
	if (is_shadow(orig) &&
	    sm_->count_possibly_greater_than_one(orig))
		return make_pair(bm_->write_lock(orig), false);

	read_ref src = bm_->read_lock(orig, v);
	write_ref dest = bm_->write_lock_zero(sm_->new_block(), v);
	::memcpy(dest->data_, src->data_, BlockSize);

	ref_t count = sm_->get_count(orig);
	if (count == 0)
		throw runtime_error("shadowing free block");
	sm_->dec(orig);
	add_shadow(dest->location_);
	return make_pair(dest, count > 1);
}

template <uint32_t BlockSize>
typename transaction_manager<BlockSize>::read_ref
transaction_manager<BlockSize>::read_lock(block_address b)
{
	return bm_->read_lock(b);
}

template <uint32_t BlockSize>
typename transaction_manager<BlockSize>::read_ref
transaction_manager<BlockSize>::read_lock(block_address b, validator v)
{
	return bm_->read_lock(b, v);
}

template <uint32_t BlockSize>
void
transaction_manager<BlockSize>::add_shadow(block_address b)
{
	shadows_.insert(b);
}

template <uint32_t BlockSize>
void
transaction_manager<BlockSize>::remove_shadow(block_address b)
{
	shadows_.erase(b);
}

template <uint32_t BlockSize>
bool
transaction_manager<BlockSize>::is_shadow(block_address b) const
{
	return shadows_.count(b) > 0;
}

template <uint32_t BlockSize>
void
transaction_manager<BlockSize>::wipe_shadow_table()
{
	shadows_.clear();
}

//----------------------------------------------------------------

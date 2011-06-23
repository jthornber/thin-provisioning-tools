#include "transaction_manager.h"

#include <string.h>

using namespace boost;
using namespace persistent_data;

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
void
transaction_manager<BlockSize>::reserve_block(block_address location)
{
	sm_->inc_block(location);
}

template <uint32_t BlockSize>
void
transaction_manager<BlockSize>::begin()
{
}

template <uint32_t BlockSize>
void
transaction_manager<BlockSize>::pre_commit()
{
	sm_->commit();
}

template <uint32_t BlockSize>
void
transaction_manager<BlockSize>::commit(write_ref superblock)
{
	bm_->flush(superblock);
	wipe_shadow_table();
}

template <uint32_t BlockSize>
block_address
transaction_manager<BlockSize>::alloc_block()
{
	return sm_->new_block();
}

template <uint32_t BlockSize>
typename transaction_manager<BlockSize>::write_ref
transaction_manager<BlockSize>::new_block()
{
	block_address b = sm_->new_block();
	add_shadow(b);
	return bm_->write_lock_zero(b);
}

template <uint32_t BlockSize>
typename transaction_manager<BlockSize>::write_ref
transaction_manager<BlockSize>::new_block(block_validator const &v)
{
	block_address b = sm_->new_block();
	add_shadow(b);
	return bm_->write_lock_zero(b, v);
}

template <uint32_t BlockSize>
typename transaction_manager<BlockSize>::write_ref
transaction_manager<BlockSize>::shadow(block_address orig, bool &inc_children)
{
	if (is_shadow(orig) &&
	    sm_->count_possibly_greater_than_one(orig)) {
		inc_children = false;
		return bm_->write_lock(orig);
	}

	auto src = bm_->read_lock(orig);
	auto dest = bm_->write_lock_zero(sm_->new_block());
	::memcpy(dest->data_, src->data_, BlockSize);

	ref_t count = sm_->get_count(orig);
	sm_->dec_block(orig);
	inc_children = count > 1;
	add_shadow(dest->location_);
	return dest;
}

template <uint32_t BlockSize>
typename transaction_manager<BlockSize>::write_ref
transaction_manager<BlockSize>::shadow(block_address orig, block_validator const &v, bool &inc_children)
{
	if (is_shadow(orig) &&
	    sm_->count_possibly_greater_than_one(orig)) {
		inc_children = false;
		return bm_->write_lock(orig);
	}

	auto src = bm_->read_lock(orig, v);
	auto dest = bm_->write_lock_zero(sm_->new_block(), v);
	::memcpy(dest->data_, src->data_, BlockSize);

	ref_t count = sm_->get_count(orig);
	sm_->dec_block(orig);
	inc_children = count > 1;
	add_shadow(dest->location_);
	return dest;
}

template <uint32_t BlockSize>
typename transaction_manager<BlockSize>::read_ref
transaction_manager<BlockSize>::read_lock(block_address b)
{
	return bm_->read_lock(b);
}

template <uint32_t BlockSize>
typename transaction_manager<BlockSize>::read_ref
transaction_manager<BlockSize>::read_lock(block_address b, block_validator const &v)
{
	return bm_->read_lock(b, v);
}

template <uint32_t BlockSize>
void
transaction_manager<BlockSize>::inc(block_address b)
{
	sm_->inc_block(b);
}

template <uint32_t BlockSize>
void
transaction_manager<BlockSize>::dec(block_address b)
{
	sm_->dec_block(b);
}

template <uint32_t BlockSize>
uint32_t
transaction_manager<BlockSize>::ref_count(block_address b) const
{
	return sm_->get_count(b);
}

template <uint32_t BlockSize>
void
transaction_manager<BlockSize>::add_shadow(block_address b)
{
	shadows_.insert(b);
}

template <uint32_t BlockSize>
bool
transaction_manager<BlockSize>::is_shadow(block_address b)
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

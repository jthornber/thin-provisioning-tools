// Copyright (C) 2011 Red Hat, Inc. All rights reserved.
//
// This file is part of the thin-provisioning-tools source.
//
// thin-provisioning-tools is free software: you can redistribute it
// and/or modify it under the terms of the GNU General Public License
// as published by the Free Software Foundation, either version 3 of
// the License, or (at your option) any later version.
//
// thin-provisioning-tools is distributed in the hope that it will be
// useful, but WITHOUT ANY WARRANTY; without even the implied warranty
// of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License along
// with thin-provisioning-tools.  If not, see
// <http://www.gnu.org/licenses/>.

#include "persistent-data/transaction_manager.h"

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
transaction_manager::begin(block_address superblock, validator v)
{
	write_ref wr = bm_->superblock(superblock, v);
	wipe_shadow_table();
	return wr;
}

transaction_manager::write_ref
transaction_manager::new_block(validator v)
{
	optional<block_address> mb = sm_->new_block();
	if (!mb)
		throw runtime_error("transaction_manager::new_block() couldn't allocate new block");

	sm_decrementer decrementer(sm_, *mb);
	write_ref wr = bm_->write_lock_zero(*mb, v);
	add_shadow(*mb);
	decrementer.dont_bother();
	return wr;
}

pair<transaction_manager::write_ref, bool>
transaction_manager::shadow(block_address orig, validator v)
{
	bool need_inc = sm_->count_possibly_greater_than_one(orig);
	if (is_shadow(orig) && !need_inc)
		return make_pair(bm_->write_lock(orig, v), need_inc);

	read_ref src = bm_->read_lock(orig, v);
	optional<block_address> mb = sm_->new_block();
	if (!mb)
		throw runtime_error("transaction_manager::shadow() couldn't allocate new block");

	write_ref dest = bm_->write_lock_zero(*mb, v);
	::memcpy(dest.data(), src.data(), MD_BLOCK_SIZE);

	sm_->dec(orig);
	add_shadow(dest.get_location());
	return make_pair(dest, need_inc);
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

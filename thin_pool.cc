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

#include "thin_pool.h"

#include "btree_checker.h"

#include <stdexcept>
#include <sstream>
#include <iostream>
#include <set>
#include <map>

using namespace base;
using namespace std;
using namespace persistent_data;
using namespace thin_provisioning;

//----------------------------------------------------------------

thin::thin(thin_dev_t dev, thin_pool *pool)
	: dev_(dev),
	  pool_(pool)
{
}

thin_dev_t
thin::get_dev_t() const
{
	return dev_;
}

thin::maybe_address
thin::lookup(block_address thin_block)
{
	uint64_t key[2] = {dev_, thin_block};
	return pool_->md_->mappings_->lookup(key);
}

void
thin::insert(block_address thin_block, block_address data_block)
{
	uint64_t key[2] = {dev_, thin_block};
	block_time bt;
	bt.block_ = data_block;
	bt.time_ = 0;		// FIXME: use current time.
	return pool_->md_->mappings_->insert(key, bt);
}

void
thin::remove(block_address thin_block)
{
	uint64_t key[2] = {dev_, thin_block};
	pool_->md_->mappings_->remove(key);
}

void
thin::set_snapshot_time(uint32_t time)
{
	uint64_t key[1] = { dev_ };
	optional<device_details> mdetail = pool_->md_->details_->lookup(key);
	if (!mdetail)
		throw runtime_error("no such device");

	mdetail->snapshotted_time_ = time;
	pool_->md_->details_->insert(key, *mdetail);
}

block_address
thin::get_mapped_blocks() const
{
	uint64_t key[1] = { dev_ };
	optional<device_details> mdetail = pool_->md_->details_->lookup(key);
	if (!mdetail)
		throw runtime_error("no such device");

	return mdetail->mapped_blocks_;
}

void
thin::set_mapped_blocks(block_address count)
{
	uint64_t key[1] = { dev_ };
	optional<device_details> mdetail = pool_->md_->details_->lookup(key);
	if (!mdetail)
		throw runtime_error("no such device");

	mdetail->mapped_blocks_ = count;
	pool_->md_->details_->insert(key, *mdetail);
}

//--------------------------------

thin_pool::thin_pool(metadata::ptr md)
	: md_(md)
{
}

thin_pool::~thin_pool()
{

}

void
thin_pool::create_thin(thin_dev_t dev)
{
	uint64_t key[1] = {dev};

	if (device_exists(dev))
		throw std::runtime_error("Device already exists");

	single_mapping_tree::ptr new_tree(new single_mapping_tree(md_->tm_, block_time_ref_counter(md_->data_sm_)));
	md_->mappings_top_level_->insert(key, new_tree->get_root());
	md_->mappings_->set_root(md_->mappings_top_level_->get_root()); // FIXME: ugly

	// FIXME: doesn't set up the device details
}

void
thin_pool::create_snap(thin_dev_t dev, thin_dev_t origin)
{
	uint64_t snap_key[1] = {dev};
	uint64_t origin_key[1] = {origin};

	optional<uint64_t> mtree_root = md_->mappings_top_level_->lookup(origin_key);
	if (!mtree_root)
		throw std::runtime_error("unknown origin");

	single_mapping_tree otree(md_->tm_, *mtree_root,
				  block_time_ref_counter(md_->data_sm_));

	single_mapping_tree::ptr clone(otree.clone());
	md_->mappings_top_level_->insert(snap_key, clone->get_root());
	md_->mappings_->set_root(md_->mappings_top_level_->get_root()); // FIXME: ugly

	md_->sb_.time_++;

	thin::ptr o = open_thin(origin);
	thin::ptr s = open_thin(dev);
	o->set_snapshot_time(md_->sb_.time_);
	s->set_snapshot_time(md_->sb_.time_);
	s->set_mapped_blocks(o->get_mapped_blocks());
}

void
thin_pool::del(thin_dev_t dev)
{
	uint64_t key[1] = {dev};
	md_->mappings_top_level_->remove(key);
}

void
thin_pool::set_transaction_id(uint64_t id)
{
	md_->sb_.trans_id_ = id;
}

uint64_t
thin_pool::get_transaction_id() const
{
	return md_->sb_.trans_id_;
}

block_address
thin_pool::get_held_root() const
{
	return md_->sb_.held_root_;
}

block_address
thin_pool::alloc_data_block()
{
	optional<block_address> mb = md_->data_sm_->new_block();
	if (!mb)
		throw runtime_error("couldn't allocate new block");

	return *mb;
}

void
thin_pool::free_data_block(block_address b)
{
	md_->data_sm_->dec(b);
}

block_address
thin_pool::get_nr_free_data_blocks() const
{
	return md_->data_sm_->get_nr_free();
}

sector_t
thin_pool::get_data_block_size() const
{
	return md_->sb_.data_block_size_;
}

block_address
thin_pool::get_data_dev_size() const
{
	return md_->data_sm_->get_nr_blocks();
}

thin::ptr
thin_pool::open_thin(thin_dev_t dev)
{
	uint64_t key[1] = {dev};
	optional<device_details> mdetails = md_->details_->lookup(key);
	if (!mdetails)
		throw runtime_error("no such device");

	thin *ptr = new thin(dev, this);
	thin::ptr r(ptr);
	return r;
}

bool
thin_pool::device_exists(thin_dev_t dev) const
{
	uint64_t key[1] = {dev};
	return md_->details_->lookup(key);
}

//----------------------------------------------------------------

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

#include "persistent-data/math_utils.h"
#include "thin-provisioning/thin_pool.h"

#include <stdexcept>

using namespace base;
using namespace std;
using namespace persistent_data;
using namespace thin_provisioning;

//----------------------------------------------------------------

thin::thin(thin_dev_t dev, thin_pool &pool)
	: dev_(dev),
	  pool_(pool),
	  details_(pool.get_transaction_id(), pool.get_time()),
	  open_count_(1),
	  changed_(true)
{
}

thin::thin(thin_dev_t dev, thin_pool &pool,
	   device_tree_detail::device_details const &details)
	: dev_(dev),
	  pool_(pool),
	  details_(details),
	  open_count_(1),
	  changed_(false)
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
	mapping_tree::maybe_value m = pool_.md_->mappings_->lookup(key);
	if (!m)
		return thin::maybe_address();

	lookup_result r;
	r.block_ = m->block_;
	r.shared_ = m->time_ < details_.snapshotted_time_;
	return r;
}

bool
thin::insert(block_address thin_block, block_address data_block)
{
	uint64_t key[2] = {dev_, thin_block};

	++details_.mapped_blocks_;
	changed_ = true;

	mapping_tree_detail::block_time bt;
	bt.block_ = data_block;
	bt.time_ = pool_.get_time();
	return pool_.md_->mappings_->insert(key, bt);
}

void
thin::remove(block_address thin_block)
{
	uint64_t key[2] = {dev_, thin_block};
	pool_.md_->mappings_->remove(key);

	--details_.mapped_blocks_;
	changed_ = true;
}

void
thin::set_snapshot_time(uint32_t time)
{
	details_.snapshotted_time_ = time;
	changed_ = true;
}

block_address
thin::get_mapped_blocks() const
{
	return details_.mapped_blocks_;
}

void
thin::set_mapped_blocks(block_address count)
{
	details_.mapped_blocks_ = count;
	changed_ = true;
}

//--------------------------------

thin_pool::thin_pool(block_manager::ptr bm)
{
	md_ = metadata::ptr(new metadata(bm, true));
}

thin_pool::thin_pool(block_manager::ptr bm,
		     sector_t data_block_size,
		     block_address nr_data_blocks)
{
	md_ = metadata::ptr(new metadata(bm,
					 metadata::CREATE,
					 data_block_size,
					 nr_data_blocks));
	md_->commit();
}

thin_pool::~thin_pool()
{

}

void
thin_pool::create_thin(thin_dev_t dev)
{
	uint64_t key[1] = {dev};

	if (device_exists(dev))
		throw std::runtime_error("device already exists");

	single_mapping_tree::ptr new_tree(new single_mapping_tree(*md_->tm_,
								  mapping_tree_detail::block_time_ref_counter(md_->data_sm_)));
	md_->mappings_top_level_->insert(key, new_tree->get_root());
	md_->mappings_->set_root(md_->mappings_top_level_->get_root()); // FIXME: ugly

	thin::ptr r = create_device(dev);
	close_device(r);
}

void
thin_pool::create_snap(thin_dev_t dev, thin_dev_t origin)
{
	uint64_t snap_key[1] = {dev};
	uint64_t origin_key[1] = {origin};

	if (device_exists(dev))
		throw std::runtime_error("device already exists");

	// find the mapping tree of the origin
	dev_tree::maybe_value mtree_root = md_->mappings_top_level_->lookup(origin_key);
	if (!mtree_root)
		throw std::runtime_error("unknown origin");
	single_mapping_tree otree(*md_->tm_, *mtree_root,
				  mapping_tree_detail::block_time_ref_counter(md_->data_sm_));

	// clone the origin
	single_mapping_tree::ptr clone(otree.clone());
	md_->mappings_top_level_->insert(snap_key, clone->get_root());
	md_->mappings_->set_root(md_->mappings_top_level_->get_root()); // FIXME: ugly

	md_->sb_.time_++;

	// create details for the snapshot
	thin::ptr s = create_device(dev);
	set_snapshot_details(s, origin);
	close_device(s);
}

void
thin_pool::del(thin_dev_t dev)
{
	uint64_t key[1] = {dev};

	thin::ptr td = open_device(dev);
	if (td->open_count_ > 1) {
		close_device(td);
		throw std::runtime_error("device busy");
	}

	thin_devices_.erase(dev);

	dev_tree::maybe_value mtree_root = md_->mappings_top_level_->lookup(key);
	if (!device_exists(dev) || !mtree_root)
		throw std::runtime_error("unknown device");

	// TODO: trigger subtree deletion from the mtree_ref_counter,
	// like the kenrel subtree_dec() does.
	single_mapping_tree mtree(*md_->tm_, *mtree_root,
				  mapping_tree_detail::block_time_ref_counter(md_->data_sm_));
	mtree.destroy();

	md_->details_->remove(key);
	md_->mappings_top_level_->remove(key);
	md_->mappings_->set_root(md_->mappings_top_level_->get_root()); // FIXME: ugly
}

void
thin_pool::commit()
{
	write_changed_details();
	md_->commit();
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

void
thin_pool::reserve_metadata_snap()
{
	if (md_->sb_.metadata_snap_)
		throw std::runtime_error("pool metadata snapshot already exists.");

	commit();

	md_->metadata_sm_->inc(superblock_detail::SUPERBLOCK_LOCATION);
	transaction_manager::write_ref wr = md_->tm_->shadow(
			superblock_detail::SUPERBLOCK_LOCATION,
			superblock_validator()).first;

	superblock_detail::superblock sb;
	superblock_detail::superblock_disk *sbd = reinterpret_cast<superblock_detail::superblock_disk *>(wr.data());
	superblock_detail::superblock_traits::unpack(*sbd, sb);

	memset(sb.data_space_map_root_, 0, superblock_detail::SPACE_MAP_ROOT_SIZE);
	memset(sb.metadata_space_map_root_, 0, superblock_detail::SPACE_MAP_ROOT_SIZE);
	md_->metadata_sm_->inc(sb.data_mapping_root_);
	md_->metadata_sm_->inc(sb.device_details_root_);

	superblock_detail::superblock_traits::pack(sb, *sbd);

	md_->sb_.metadata_snap_ = wr.get_location();
}

void
thin_pool::release_metadata_snap()
{
	if (!md_->sb_.metadata_snap_)
		throw std::runtime_error("No pool metadata snapshot found");

	superblock_detail::superblock sb = read_superblock(md_->tm_->get_bm(),
							   md_->sb_.metadata_snap_);
	device_tree dtree(*md_->tm_, sb.device_details_root_,
			  device_tree_detail::device_details_traits::ref_counter());
	dtree.destroy();
	mapping_tree mtree(*md_->tm_, sb.data_mapping_root_,
			   mapping_tree_detail::block_traits::ref_counter(md_->tm_->get_sm()));
	mtree.destroy();
	md_->metadata_sm_->dec(md_->sb_.metadata_snap_);

	md_->sb_.metadata_snap_ = 0;
}

block_address
thin_pool::get_metadata_snap() const
{
	return md_->sb_.metadata_snap_;
}

block_address
thin_pool::alloc_data_block()
{
	space_map::maybe_block mb = md_->data_sm_->new_block();
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

thin_provisioning::sector_t
thin_pool::get_data_block_size() const
{
	return md_->sb_.data_block_size_;
}

block_address
thin_pool::get_data_dev_size() const
{
	return md_->data_sm_->get_nr_blocks();
}

uint32_t
thin_pool::get_time() const
{
	return md_->sb_.time_;
}

thin::ptr
thin_pool::open_thin(thin_dev_t dev)
{
	return open_device(dev);
}

void
thin_pool::close_thin(thin::ptr td)
{
	close_device(td);
}

bool
thin_pool::device_exists(thin_dev_t dev) const
{
	uint64_t key[1] = {dev};
	return !!md_->details_->lookup(key);
}

thin::ptr
thin_pool::create_device(thin_dev_t dev)
{
	device_map::iterator it = thin_devices_.find(dev);
	if (it != thin_devices_.end())
		throw std::runtime_error("device already exists");

	thin::ptr td(new thin(dev, *this));
	thin_devices_[dev] = td;
	return td;
}

thin::ptr
thin_pool::open_device(thin_dev_t dev)
{
	device_map::iterator it = thin_devices_.find(dev);
	if (it != thin_devices_.end()) {
		thin::ptr td = it->second;
		td->open_count_++;
		return td;
	}

	uint64_t key[1] = {dev};
	device_tree::maybe_value details = md_->details_->lookup(key);
	if (!details)
		throw std::runtime_error("no such device");

	thin::ptr td(new thin(dev, *this, *details));
	thin_devices_[dev] = td;
	return td;
}

void
thin_pool::close_device(thin::ptr td)
{
	td->open_count_--;
}

void
thin_pool::set_snapshot_details(thin::ptr snap, thin_dev_t origin)
{
	thin::ptr o = open_device(origin);
	o->set_snapshot_time(md_->sb_.time_);
	snap->set_snapshot_time(md_->sb_.time_);
	snap->set_mapped_blocks(o->get_mapped_blocks());
	close_device(o);
}

void
thin_pool::write_changed_details()
{
	for (auto it = thin_devices_.cbegin(); it != thin_devices_.cend(); ) {
		uint64_t key[1] = {it->first};
		thin::ptr td = it->second;

		if (td->changed_) {
			md_->details_->insert(key, td->details_);
			td->changed_ = false;
		}

		if (!td->open_count_)
			it = thin_devices_.erase(it);
		else
			++it;
	}
}

//----------------------------------------------------------------

void
thin_provisioning::process_read(thin::ptr td, thin_pool::ptr tp,
				sector_t offset)
{
	block_address blocknr = base::div_up<sector_t>(offset, tp->get_data_block_size());
	td->lookup(blocknr);
}

void
thin_provisioning::process_write(thin::ptr td, thin_pool::ptr tp,
				 sector_t offset)
{
	block_address blocknr = base::div_up<sector_t>(offset, tp->get_data_block_size());
	thin::maybe_address result = td->lookup(blocknr);
	if (!!result && !result->shared_)
		return;
	// TODO: handle out-of-space errors
	block_address data_block = tp->alloc_data_block();
	td->insert(blocknr, data_block);
}

void
thin_provisioning::process_discard(thin::ptr td, thin_pool::ptr tp,
				   sector_t offset)
{
	block_address blocknr = base::div_up<sector_t>(offset, tp->get_data_block_size());
	thin::maybe_address result = td->lookup(blocknr);
	if (!result)
		return;
	td->remove(blocknr);
	if (!result->shared_)
		tp->free_data_block(result->block_);
}

//----------------------------------------------------------------

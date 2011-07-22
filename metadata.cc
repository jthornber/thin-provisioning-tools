#include "metadata.h"

#include <stdexcept>

using namespace persistent_data;
using namespace thin_provisioning;

//----------------------------------------------------------------

namespace {
	uint32_t const SUPERBLOCK_MAGIC = 27022010;
	block_address const SUPERBLOCK_LOCATION = 0;
        uint32_t const VERSION = 1;
	unsigned const METADATA_CACHE_SIZE = 1024;
        unsigned const SECTOR_TO_BLOCK_SHIFT = 3;
}

//----------------------------------------------------------------

thin::thin(thin_dev_t dev, metadata *metadata)
	: dev_(dev),
	  metadata_(metadata)
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
	return metadata_->mappings_.lookup(key);
}

void
thin::insert(block_address thin_block, block_address data_block)
{
	uint64_t key[2] = {dev_, thin_block};
	return metadata_->mappings_.insert(key, data_block);
}

void
thin::remove(block_address thin_block)
{
	uint64_t key[2] = {dev_, thin_block};
	metadata_->mappings_.remove(key);
}

void
thin::set_snapshot_time(uint32_t time)
{
	uint64_t key[1] = { dev_ };
	auto mdetail = metadata_->details_.lookup(key);
	if (!mdetail)
		throw runtime_error("no such device");

	mdetail->snapshotted_time_ = time;
	metadata_->details_.insert(key, *mdetail);
}

block_address
thin::get_mapped_blocks() const
{
	uint64_t key[1] = { dev_ };
	auto mdetail = metadata_->details_.lookup(key);
	if (!mdetail)
		throw runtime_error("no such device");

	return mdetail->mapped_blocks_;
}

void
thin::set_mapped_blocks(block_address count)
{
	uint64_t key[1] = { dev_ };
	auto mdetail = metadata_->details_.lookup(key);
	if (!mdetail)
		throw runtime_error("no such device");

	mdetail->mapped_blocks_ = count;
	metadata_->details_.insert(key, *mdetail);
}

//--------------------------------

metadata::metadata(transaction_manager<MD_BLOCK_SIZE>::ptr tm,
		   block_address superblock,
		   sector_t data_block_size,
		   block_address nr_data_blocks,
		   bool create)
	: superblock_(superblock),
	  tm_(tm),
	  details_(tm, typename device_details_traits::ref_counter()),
	  mappings_top_level_(tm, mtree_ref_counter<MD_BLOCK_SIZE>(tm)),
	  mappings_(tm, space_map_ref_counter(data_sm_))
{
	::memset(&sb_, 0, sizeof(sb_));
	sb_.data_mapping_root_ = mappings_.get_root();
	sb_.device_details_root_ = details_.get_root();
	sb_.metadata_block_size_ = MD_BLOCK_SIZE;
	sb_.metadata_nr_blocks_ = tm->get_bm()->get_nr_blocks();
}

metadata::~metadata()
{

}

void
metadata::commit()
{
	sb_.data_mapping_root_ = mappings_.get_root();
	sb_.device_details_root_ = details_.get_root();

	auto superblock = tm_->get_bm()->superblock(superblock_);
        auto disk = reinterpret_cast<superblock_disk *>(superblock.data());
	superblock_traits::pack(sb_, *disk);
}

void
metadata::create_thin(thin_dev_t dev)
{
	uint64_t key[1] = {dev};

	if (device_exists(dev))
		throw std::runtime_error("Device already exists");

	single_mapping_tree::ptr new_tree(new single_mapping_tree(tm_, space_map_ref_counter(data_sm_)));
	mappings_top_level_.insert(key, new_tree->get_root());
	mappings_.set_root(mappings_top_level_.get_root()); // FIXME: ugly
}

void
metadata::create_snap(thin_dev_t dev, thin_dev_t origin)
{
	uint64_t snap_key[1] = {dev};
	uint64_t origin_key[1] = {origin};

	auto mtree_root = mappings_top_level_.lookup(origin_key);
	if (!mtree_root)
		throw std::runtime_error("unknown origin");

	single_mapping_tree otree(tm_, *mtree_root,
				  space_map_ref_counter(data_sm_));

	single_mapping_tree::ptr clone(otree.clone());
	mappings_top_level_.insert(snap_key, clone->get_root());
	mappings_.set_root(mappings_top_level_.get_root()); // FIXME: ugly

	sb_.time_++;

	auto o = open_thin(origin);
	auto s = open_thin(dev);
	o->set_snapshot_time(sb_.time_);
	s->set_snapshot_time(sb_.time_);
	s->set_mapped_blocks(o->get_mapped_blocks());
}

void
metadata::del(thin_dev_t dev)
{
	uint64_t key[1] = {dev};
	mappings_top_level_.remove(key);
}

void
metadata::set_transaction_id(uint64_t id)
{
	sb_.trans_id_ = id;
}

uint64_t
metadata::get_transaction_id() const
{
	return sb_.trans_id_;
}

block_address
metadata::get_held_root() const
{
	return sb_.held_root_;
}

block_address
metadata::alloc_data_block()
{
	return data_sm_->new_block();
}

void
metadata::free_data_block(block_address b)
{
	data_sm_->dec(b);
}

block_address
metadata::get_nr_free_data_blocks() const
{
	return data_sm_->get_nr_free();
}

sector_t
metadata::get_data_block_size() const
{
	return sb_.data_block_size_;
}

block_address
metadata::get_data_dev_size() const
{
	return data_sm_->get_nr_blocks();
}

thin::ptr
metadata::open_thin(thin_dev_t dev)
{
	uint64_t key[1] = {dev};
	auto mdetails = details_.lookup(key);
	if (!mdetails)
		throw runtime_error("no such device");

	thin *ptr = new thin(dev, this);
	thin::ptr r(ptr);
	return r;
}

bool
metadata::device_exists(thin_dev_t dev) const
{
	uint64_t key[1] = {dev};
	return details_.lookup(key);
}

//----------------------------------------------------------------

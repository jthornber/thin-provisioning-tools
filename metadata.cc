#include "metadata.h"

#include <stdexcept>

using namespace persistent_data;
using namespace multisnap;

//----------------------------------------------------------------

namespace {
	typedef uint8_t __le8;
	typedef uint8_t __u8;
	typedef uint32_t __le32;
	typedef uint64_t __le64;



	uint32_t const SUPERBLOCK_MAGIC = 27022010;
	block_address const SUPERBLOCK_LOCATION = 0;
        uint32_t const VERSION = 1;
	unsigned const METADATA_CACHE_SIZE = 1024;
        unsigned const SECTOR_TO_BLOCK_SHIFT = 3;
	unsigned const SPACE_MAP_ROOT_SIZE = 128;

	struct multisnap_super_block {
		__le32 csum_;
		__le32 flags_;
		__le64 blocknr_; /* this block number, dm_block_t */

		__u8 uuid_[16];
		__le64 magic_;
		__le32 version_;
		__le32 time_;

		__le64 trans_id_;
		/* root for userspace's transaction (for migration and friends) */
		__le64 held_root_;

		__u8 data_space_map_root_[SPACE_MAP_ROOT_SIZE];
		__u8 metadata_space_map_root_[SPACE_MAP_ROOT_SIZE];

		/* 2 level btree mapping (dev_id, (dev block, time)) -> data block */
		__le64 data_mapping_root_;

		/* device detail root mapping dev_id -> device_details */
		__le64 device_details_root_;

		__le32 data_block_size_; /* in 512-byte sectors */

		__le32 metadata_block_size_; /* in 512-byte sectors */
		__le64 metadata_nr_blocks_;

		__le32 compat_flags_;
		__le32 incompat_flags_;
	} __attribute__ ((packed));

	struct device_details {
		__le64 dev_size_;
		__le64 mapped_blocks_;
		__le64 transaction_id_;  /* when created */
		__le32 creation_time_;
		__le32 snapshotted_time_;
	} __attribute__ ((packed));
}

//----------------------------------------------------------------

metadata::thin::maybe_address
metadata::thin::lookup(block_address thin_block)
{
	uint64_t key[2] = {dev_, thin_block};
	return metadata_->mappings_.lookup(key);
}

void
metadata::thin::insert(block_address thin_block, block_address data_block)
{
	uint64_t key[2] = {dev_, thin_block};
	return metadata_->mappings_.insert(key, data_block);
}

void
metadata::thin::remove(block_address thin_block)
{
	uint64_t key[2] = {dev_, thin_block};
	metadata_->mappings_.remove(key);
}
#if 0
//--------------------------------

metadata::metadata(std::string const &metadata_dev,
		   sector_t data_block_size,
		   block_address nr_data_blocks)
{

}

metadata::~metadata()
{

}

void
metadata::commit()
{

}
#endif
void
metadata::create_thin(dev_t dev)
{
	uint64_t key[1] = {dev};

	if (device_exists(dev))
		throw std::runtime_error("Device already exists");

	single_mapping_tree::ptr new_tree(new single_mapping_tree(tm_));
	mappings_top_level_.insert(key, *new_tree);
	mappings_.set_root(mappings_top_level_.get_root()); // FIXME: ugly
}

void
metadata::create_snap(dev_t dev, dev_t origin)
{
	uint64_t snap_key[1] = {dev};
	uint64_t origin_key[1] = {origin};

	auto mtree = mappings_top_level_.lookup(origin_key);
	if (!mtree)
		throw std::runtime_error("unknown origin");

	single_mapping_tree::ptr clone(mtree->clone());
	mappings_top_level_.insert(snap_key, *clone);
	mappings_.set_root(mappings_top_level_.get_root()); // FIXME: ugly

	time_++;

	auto o = open(origin);
	auto s = open(dev);
	o->set_snapshot_time(time_);
	s->set_snapshot_time(time_);
	s->set_mapped_blocks(o->get_mapped_blocks());
}

void
metadata::del(dev_t dev)
{
	uint64_t key[1] = {dev};
	mappings_top_level_.remove(key);
}

#if 0
void
metadata::set_transaction_id(uint64_t id)
{

}

uint64_t
metadata::get_transaction_id() const
{

}

block_address
metadata::get_held_root() const
{

}

thin_ptr
metadata::open_device(dev_t)
{

}

block_address
metadata::alloc_data_block()
{

}

void
metadata::free_data_block(block_address b)
{

}

block_address
metadata::get_nr_free_data_blocks() const
{

}

sector_t
metadata::get_data_block_size() const
{

}

block_address
metadata::get_data_dev_size() const
{

}
#endif
bool
metadata::device_exists(dev_t dev) const
{
	uint64_t key[1] = {dev};
	auto mval = details_.lookup(key);
	return mval;
}

//----------------------------------------------------------------

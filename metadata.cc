#include "metadata.h"

#include "btree_checker.h"
#include "core_map.h"

#include <stdexcept>
#include <sstream>
#include <iostream>
#include <set>
#include <map>

using namespace std;
using namespace persistent_data;
using namespace thin_provisioning;

//----------------------------------------------------------------

namespace {
	uint32_t const SUPERBLOCK_MAGIC = 27022010;
	block_address const SUPERBLOCK_LOCATION = 0;
        uint32_t const VERSION = 1;
	unsigned const METADATA_CACHE_SIZE = 1024;
        unsigned const SECTOR_TO_BLOCK_SHIFT = 3;

	// FIXME: get the file size
	unsigned const NR_BLOCKS = 1024;

	transaction_manager::ptr
	open_tm(string const &dev_path) {
		block_manager<>::ptr bm(new block_manager<>(dev_path, NR_BLOCKS));
		space_map::ptr sm(new core_map(NR_BLOCKS));
		transaction_manager::ptr tm(new transaction_manager(bm, sm));
		return tm;
	}

	superblock read_superblock(block_manager<>::ptr bm) {
		superblock sb;
		block_manager<>::read_ref r = bm->read_lock(SUPERBLOCK_LOCATION);
		superblock_disk const *sbd = reinterpret_cast<superblock_disk const *>(&r.data());
		superblock_traits::unpack(*sbd, sb);
		return sb;
	}

	// As well as the standard btree checks, we build up a set of what
	// devices having mappings defined, which can later be cross
	// referenced with the details tree.  A separate block_counter is
	// used to later verify the data space map.
	class mapping_validator : public btree_checker<2, block_traits> {
	public:
		typedef boost::shared_ptr<mapping_validator> ptr;

		mapping_validator(block_counter &metadata_counter, block_counter &data_counter)
			: btree_checker<2, block_traits>(metadata_counter),
			  data_counter_(data_counter) {
		}

		// Sharing can only occur in level 1 nodes.
		// FIXME: not true once we start having held roots.
		bool visit_internal_leaf(unsigned level,
					 optional<uint64_t> key,
					 btree_detail::node_ref<uint64_traits> const &n) {

			bool r = btree_checker<2, block_traits>::visit_internal_leaf(level, key, n);
			if (!r && level == 0) {
				throw runtime_error("unexpected sharing in level 0 of mapping tree.");
			}

			for (unsigned i = 0; i < n.get_nr_entries(); i++)
				devices_.insert(n.key_at(i));

			return r;
		}

		bool visit_leaf(unsigned level,
				optional<uint64_t> key,
				btree_detail::node_ref<block_traits> const &n) {
			bool r = btree_checker<2, block_traits>::visit_leaf(level, key, n);

			if (r)
				for (unsigned i = 0; i < n.get_nr_entries(); i++)
					data_counter_.inc(n.value_at(i).block_);

			return r;
		}

		set<uint64_t> const &get_devices() const {
			return devices_;
		}

	private:
		block_counter &data_counter_;
		set<uint64_t> devices_;
	};

	class details_validator : public btree_checker<1, device_details_traits> {
	public:
		typedef boost::shared_ptr<details_validator> ptr;

		details_validator(block_counter &counter)
			: btree_checker<1, device_details_traits>(counter) {
		}

		bool visit_leaf(unsigned level,
				optional<uint64_t> key,
				btree_detail::node_ref<device_details_traits> const &n) {
			bool r = btree_checker<1, device_details_traits>::visit_leaf(level, key, n);

			if (r)
				for (unsigned i = 0; i < n.get_nr_entries(); i++)
					devices_.insert(n.key_at(i));

			return r;
		}

		set<uint64_t> const &get_devices() const {
			return devices_;
		}

	private:
		set<uint64_t> devices_;
	};
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
	block_time bt;
	bt.block_ = data_block;
	bt.time_ = 0;		// FIXME: use current time.
	return metadata_->mappings_.insert(key, bt);
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
	optional<device_details> mdetail = metadata_->details_.lookup(key);
	if (!mdetail)
		throw runtime_error("no such device");

	mdetail->snapshotted_time_ = time;
	metadata_->details_.insert(key, *mdetail);
}

block_address
thin::get_mapped_blocks() const
{
	uint64_t key[1] = { dev_ };
	optional<device_details> mdetail = metadata_->details_.lookup(key);
	if (!mdetail)
		throw runtime_error("no such device");

	return mdetail->mapped_blocks_;
}

void
thin::set_mapped_blocks(block_address count)
{
	uint64_t key[1] = { dev_ };
	optional<device_details> mdetail = metadata_->details_.lookup(key);
	if (!mdetail)
		throw runtime_error("no such device");

	mdetail->mapped_blocks_ = count;
	metadata_->details_.insert(key, *mdetail);
}

//--------------------------------

metadata::metadata(std::string const &dev_path)
	: tm_(open_tm(dev_path)),
	  sb_(read_superblock(tm_->get_bm())),
	  metadata_sm_(open_metadata_sm(tm_, static_cast<void *>(&sb_.metadata_space_map_root_))),
	  data_sm_(open_disk_sm(tm_, static_cast<void *>(&sb_.data_space_map_root_))),
	  details_(tm_, sb_.device_details_root_, device_details_traits::ref_counter()),
	  mappings_top_level_(tm_, sb_.data_mapping_root_, mtree_ref_counter(tm_)),
	  mappings_(tm_, sb_.data_mapping_root_, block_time_ref_counter(data_sm_))
{
#if 0
	::memset(&sb_, 0, sizeof(sb_));
	sb_.data_mapping_root_ = mappings_.get_root();
	sb_.device_details_root_ = details_.get_root();
	sb_.metadata_block_size_ = MD_BLOCK_SIZE;
	sb_.metadata_nr_blocks_ = tm_->get_bm()->get_nr_blocks();
#endif
}

metadata::~metadata()
{

}

void
metadata::commit()
{
	sb_.data_mapping_root_ = mappings_.get_root();
	sb_.device_details_root_ = details_.get_root();

	write_ref superblock = tm_->get_bm()->superblock(SUPERBLOCK_LOCATION);
        superblock_disk *disk = reinterpret_cast<superblock_disk *>(superblock.data());
	superblock_traits::pack(sb_, *disk);
}

void
metadata::create_thin(thin_dev_t dev)
{
	uint64_t key[1] = {dev};

	if (device_exists(dev))
		throw std::runtime_error("Device already exists");

	single_mapping_tree::ptr new_tree(new single_mapping_tree(tm_, block_time_ref_counter(data_sm_)));
	mappings_top_level_.insert(key, new_tree->get_root());
	mappings_.set_root(mappings_top_level_.get_root()); // FIXME: ugly
}

void
metadata::create_snap(thin_dev_t dev, thin_dev_t origin)
{
	uint64_t snap_key[1] = {dev};
	uint64_t origin_key[1] = {origin};

	optional<uint64_t> mtree_root = mappings_top_level_.lookup(origin_key);
	if (!mtree_root)
		throw std::runtime_error("unknown origin");

	single_mapping_tree otree(tm_, *mtree_root,
				  block_time_ref_counter(data_sm_));

	single_mapping_tree::ptr clone(otree.clone());
	mappings_top_level_.insert(snap_key, clone->get_root());
	mappings_.set_root(mappings_top_level_.get_root()); // FIXME: ugly

	sb_.time_++;

	thin::ptr o = open_thin(origin);
	thin::ptr s = open_thin(dev);
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
	optional<device_details> mdetails = details_.lookup(key);
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

namespace {
	optional<error_set::ptr>
	check_ref_counts(string const &desc, block_counter const &counts,
			 space_map::ptr sm) {
		error_set::ptr errors(new error_set(desc));

		bool bad = false;
		for (block_address b = 0; b < sm->get_nr_blocks(); b++) {
			uint32_t actual = sm->get_count(b);
			uint32_t expected = counts.get_count(b);

			if (actual != expected) {
				ostringstream out;
				out << b << ": was " << actual
				    << ", expected " << expected;
				errors->add_child(out.str());
				bad = true;
			}
		}

		return bad ? optional<error_set::ptr>(errors) : optional<error_set::ptr>();
	}
}

boost::optional<error_set::ptr>
metadata::check()
{
	error_set::ptr errors(new error_set("Errors in metadata"));

	block_counter metadata_counter, data_counter;

	mapping_validator::ptr mv(new mapping_validator(metadata_counter,
							data_counter));
	mappings_.visit(mv);
	set<uint64_t> const &mapped_devs = mv->get_devices();

	details_validator::ptr dv(new details_validator(metadata_counter));
	details_.visit(dv);
	set<uint64_t> const &details_devs = dv->get_devices();

	for (set<uint64_t>::const_iterator it = mapped_devs.begin(); it != mapped_devs.end(); ++it)
		if (details_devs.count(*it) == 0) {
			ostringstream out;
			out << "mapping exists for device " << *it
			    << ", yet there is no entry in the details tree.";
			throw runtime_error(out.str());
		}

	metadata_counter.inc(SUPERBLOCK_LOCATION);
	metadata_sm_->check(metadata_counter);
	data_sm_->check(metadata_counter);
	errors->add_child(check_ref_counts("Errors in metadata block reference counts",
					   metadata_counter, metadata_sm_));
	errors->add_child(check_ref_counts("Errors in data block reference counts",
					   data_counter, data_sm_));

	return (errors->get_children().size() > 0) ?
		optional<error_set::ptr>(errors) :
		optional<error_set::ptr>();
}

//----------------------------------------------------------------

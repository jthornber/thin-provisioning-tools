#include "metadata.h"

#include "core_map.h"

#include <stdexcept>
#include <sstream>
#include <iostream>

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

	transaction_manager<4096>::ptr
	open_tm(string const &dev_path) {
		block_manager<4096>::ptr bm(new block_manager<4096>(dev_path, NR_BLOCKS));
		space_map::ptr sm(new core_map(NR_BLOCKS));
		transaction_manager<4096>::ptr tm(new transaction_manager<4096>(bm, sm));
		return tm;
	}

	superblock read_superblock(block_manager<4096>::ptr bm) {
		superblock sb;
		auto r = bm->read_lock(SUPERBLOCK_LOCATION);
		superblock_disk const *sbd = reinterpret_cast<superblock_disk const *>(&r.data());
		superblock_traits::unpack(*sbd, sb);
		return sb;
	}

	//----------------------------------------------------------------
	// This class implements consistency checking for the
	// btrees in general.  It's worth summarising what is checked:
	//
	// Implemented
	// -----------
	//
	// - No block appears in the tree more than once.
	// - block_nr
	// - nr_entries < max_entries
	// - max_entries fits in block
	// - max_entries is divisible by 3
	//
	// Not implemented
	// ---------------
	//
	// - checksum
	// - leaf | internal flags (this can be inferred from siblings)
	// - nr_entries > minimum
	//----------------------------------------------------------------
	template <uint32_t Levels, typename ValueTraits, uint32_t BlockSize>
	class btree_validator : public btree<Levels, ValueTraits, BlockSize>::visitor {
	public:
		void visit_internal(unsigned level, btree_detail::node_ref<uint64_traits, BlockSize> const &n) {
			check_duplicate_block(n.get_location());
			check_block_nr(n);
			check_max_entries(n);
			check_nr_entries(n);
		}

		void visit_internal_leaf(unsigned level, btree_detail::node_ref<uint64_traits, BlockSize> const &n) {
			check_duplicate_block(n.get_location());
			check_block_nr(n);
			check_max_entries(n);
			check_nr_entries(n);
		}

		void visit_leaf(unsigned level, btree_detail::node_ref<ValueTraits, BlockSize> const &n) {
			check_duplicate_block(n.get_location());
			check_block_nr(n);
			check_max_entries(n);
			check_nr_entries(n);
		}

	private:
		void check_duplicate_block(block_address b) {
			if (seen_.count(b)) {
				ostringstream out;
				out << "duplicate block in btree: " << b;
				throw runtime_error(out.str());
			}

			seen_.insert(b);
		}

		template <typename node>
		void check_block_nr(node const &n) const {
			if (n.get_location() != n.get_block_nr()) {
				ostringstream out;
				out << "block number mismatch: actually "
				    << n.get_location()
				    << ", claims " << n.get_block_nr();
				throw runtime_error(out.str());
			}
		}

		template <typename node>
		void check_max_entries(node const &n) const {
			size_t elt_size = sizeof(uint64_t) + n.get_value_size();
			if (elt_size * n.get_max_entries() + sizeof(node_header) > BlockSize) {
				ostringstream out;
				out << "max entries too large: " << n.get_max_entries();
				throw runtime_error(out.str());
			}

			if (n.get_max_entries() % 3) {
				ostringstream out;
				out << "max entries is not divisible by 3: " << n.get_max_entries();
				throw runtime_error(out.str());
			}
		}

		template <typename node>
		void check_nr_entries(node const &n) const {
			if (n.get_nr_entries() > n.get_max_entries()) {
				ostringstream out;
				out << "bad nr_entries: "
				    << n.get_nr_entries() << " < "
				    << n.get_max_entries();
				throw runtime_error(out.str());
			}
		}

		set<block_address> seen_;
	};

	// As well as the standard btree checks, we build up a set of what
	// devices having mappings defined, which can later be cross
	// referenced with the details tree.
	class mapping_validator : public btree_validator<2, block_traits, MD_BLOCK_SIZE> {
	public:
		typedef boost::shared_ptr<mapping_validator> ptr;

		void visit_internal_leaf(unsigned level,
					 btree_detail::node_ref<uint64_traits, MD_BLOCK_SIZE> const &n) {
			btree_validator<2, block_traits, MD_BLOCK_SIZE>::visit_internal_leaf(level, n);

			for (unsigned i = 0; i < n.get_nr_entries(); i++)
				devices_.insert(n.key_at(i));
		}

		set<uint64_t> get_devices() const {
			return devices_;
		}

	private:
		set<uint64_t> devices_;
	};

	class details_validator : public btree_validator<1, device_details_traits, MD_BLOCK_SIZE> {
	public:
		typedef boost::shared_ptr<details_validator> ptr;

		void visit_leaf(unsigned level,
				btree_detail::node_ref<device_details_traits, MD_BLOCK_SIZE> const &n) {
			btree_validator<1, device_details_traits, MD_BLOCK_SIZE>::visit_leaf(level, n);

			for (unsigned i = 0; i < n.get_nr_entries(); i++)
				devices_.insert(n.key_at(i));
		}

		set<uint64_t> get_devices() const {
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

metadata::metadata(std::string const &dev_path)
	: tm_(open_tm(dev_path)),
	  sb_(read_superblock(tm_->get_bm())),
	  details_(tm_, sb_.device_details_root_, typename device_details_traits::ref_counter()),
	  mappings_top_level_(tm_, sb_.data_mapping_root_, mtree_ref_counter<MD_BLOCK_SIZE>(tm_)),
	  mappings_(tm_, sb_.data_mapping_root_, space_map_ref_counter(data_sm_))
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

	auto superblock = tm_->get_bm()->superblock(SUPERBLOCK_LOCATION);
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

void
metadata::check()
{
	mapping_validator::ptr mv(new mapping_validator);
	mappings_.visit(mv);
	auto mapped_devs = mv->get_devices();

	details_validator::ptr dv(new details_validator);
	details_.visit(dv);
	auto details_devs = dv->get_devices();

	for (auto it = mapped_devs.begin(); it != mapped_devs.end(); ++it)
		if (details_devs.count(*it) == 0) {
			ostringstream out;
			out << "mapping exists for device " << *it
			    << ", yet there is no entry in the details tree.";
			throw runtime_error(out.str());
		}
}

//----------------------------------------------------------------

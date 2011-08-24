#ifndef MULTISNAP_METADATA_H
#define MULTISNAP_METADATA_H

#include "block.h"
#include "btree.h"
#include "endian_utils.h"
#include "error_set.h"
#include "metadata_disk_structures.h"
#include "space_map_disk.h"
#include "transaction_manager.h"

#include <string>

#include <boost/shared_ptr.hpp>

//----------------------------------------------------------------

namespace thin_provisioning {
	unsigned const MD_BLOCK_SIZE = 4096;

	// FIXME: don't use namespaces in a header
	using namespace base;
	using namespace persistent_data;

	typedef uint64_t sector_t;
	typedef uint32_t thin_dev_t;

	//------------------------------------------------

	class space_map_ref_counter {
	public:
		space_map_ref_counter(space_map::ptr sm)
			: sm_(sm) {
		}

		void inc(block_address b) {
			sm_->inc(b);
		}

		void dec(block_address b) {
			sm_->dec(b);
		}

	private:
		space_map::ptr sm_;
	};

	struct block_time {
		uint64_t block_;
		uint32_t time_;
	};

	class block_time_ref_counter {
	public:
		block_time_ref_counter(space_map::ptr sm)
			: sm_(sm) {
		}

		void inc(block_time bt) {
			sm_->inc(bt.block_);
		}

		void dec(block_time bt) {
			sm_->dec(bt.block_);
		}

	private:
		space_map::ptr sm_;
	};

	struct block_traits {
		typedef base::__le64 disk_type;
		typedef block_time value_type;
		typedef block_time_ref_counter ref_counter;

		static void unpack(disk_type const &disk, value_type &value) {
			uint64_t v = to_cpu<uint64_t>(disk);
			value.block_ = v >> 24;
			value.time_ = v & ((1 << 24) - 1);
		}

		static void pack(value_type const &value, disk_type &disk) {
			uint64_t v = (value.block_ << 24) | value.time_;
			disk = base::to_disk<base::__le64>(v);
		}
	};

	//------------------------------------------------

	template <uint32_t BlockSize>
	class mtree_ref_counter {
	public:
		mtree_ref_counter(typename transaction_manager<BlockSize>::ptr tm)
			: tm_(tm) {
		}

		void inc(block_address b) {
		}

		void dec(block_address b) {
		}

	private:
		typename transaction_manager<BlockSize>::ptr tm_;
	};

	template <uint32_t BlockSize>
	struct mtree_traits {
		typedef base::__le64 disk_type;
		typedef uint64_t value_type;
		typedef mtree_ref_counter<BlockSize> ref_counter;

		static void unpack(disk_type const &disk, value_type &value) {
			value = base::to_cpu<uint64_t>(disk);
		}

		static void pack(value_type const &value, disk_type &disk) {
			disk = base::to_disk<base::__le64>(value);
		}
	};

	class metadata;
	class thin {
	public:
		typedef boost::shared_ptr<thin> ptr;
		typedef boost::optional<block_time> maybe_address;

		thin_dev_t get_dev_t() const;
		maybe_address lookup(block_address thin_block);
		void insert(block_address thin_block, block_address data_block);
		void remove(block_address thin_block);

		void set_snapshot_time(uint32_t time);

		block_address get_mapped_blocks() const;
		void set_mapped_blocks(block_address count);

	private:
		friend class metadata;
		thin(thin_dev_t dev, metadata *metadata);

		thin_dev_t dev_;
		metadata *metadata_;
	};

	class metadata {
	public:
		typedef boost::shared_ptr<metadata> ptr;
		typedef typename block_manager<MD_BLOCK_SIZE>::read_ref read_ref;
		typedef typename block_manager<MD_BLOCK_SIZE>::write_ref write_ref;

		metadata(std::string const &dev_path);
		~metadata();

		void commit();

		void create_thin(thin_dev_t dev);
		void create_snap(thin_dev_t dev, thin_dev_t origin);
		void del(thin_dev_t);

		void set_transaction_id(uint64_t id);
		uint64_t get_transaction_id() const;

		block_address get_held_root() const;

		block_address alloc_data_block();
		void free_data_block(block_address b);

		// accessors
		block_address get_nr_free_data_blocks() const;
		sector_t get_data_block_size() const;
		block_address get_data_dev_size() const;

		thin::ptr open_thin(thin_dev_t);

		// Validation and repair
		boost::optional<persistent_data::error_set::ptr> check();

	private:
		friend class thin;

		bool device_exists(thin_dev_t dev) const;

		typedef persistent_data::transaction_manager<MD_BLOCK_SIZE>::ptr tm_ptr;

		typedef persistent_data::btree<1, device_details_traits, MD_BLOCK_SIZE> detail_tree;
		typedef persistent_data::btree<1, mtree_traits<MD_BLOCK_SIZE>, MD_BLOCK_SIZE> dev_tree;
		typedef persistent_data::btree<2, block_traits, MD_BLOCK_SIZE> mapping_tree;
		typedef persistent_data::btree<1, block_traits, MD_BLOCK_SIZE> single_mapping_tree;

		// Declaration order is important here
		tm_ptr tm_;
		superblock sb_;

		// Ignoring the metadata sm for now, since we don't need it for the basic 'dump' tool
		// space_map::ptr metadata_sm_;
		sm_disk_detail::sm_disk<MD_BLOCK_SIZE>::ptr data_sm_;
		detail_tree details_;
		dev_tree mappings_top_level_;
		mapping_tree mappings_;
	};
};

//----------------------------------------------------------------

#endif

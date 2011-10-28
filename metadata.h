#ifndef METADATA_LL_H
#define METADATA_LL_H

#include "block.h"
#include "btree.h"
#include "endian_utils.h"
#include "metadata_disk_structures.h"
#include "space_map_disk.h"
#include "transaction_manager.h"

//----------------------------------------------------------------

namespace thin_provisioning {
	// FIXME: don't use namespaces in a header
	using namespace base;
	using namespace persistent_data;

	block_address const SUPERBLOCK_LOCATION = 0;

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

	class mtree_ref_counter {
	public:
		mtree_ref_counter(transaction_manager::ptr tm)
			: tm_(tm) {
		}

		void inc(block_address b) {
		}

		void dec(block_address b) {
		}

	private:
		transaction_manager::ptr tm_;
	};

	struct mtree_traits {
		typedef base::__le64 disk_type;
		typedef uint64_t value_type;
		typedef mtree_ref_counter ref_counter;

		static void unpack(disk_type const &disk, value_type &value) {
			value = base::to_cpu<uint64_t>(disk);
		}

		static void pack(value_type const &value, disk_type &disk) {
			disk = base::to_disk<base::__le64>(value);
		}
	};

	// FIXME: should these be in a sub-namespace?
	typedef persistent_data::transaction_manager::ptr tm_ptr;
	typedef persistent_data::btree<1, device_details_traits> detail_tree;
	typedef persistent_data::btree<1, mtree_traits> dev_tree;
	typedef persistent_data::btree<2, block_traits> mapping_tree;
	typedef persistent_data::btree<1, block_traits> single_mapping_tree;

	// The tools require different interfaces onto the metadata than
	// the in kernel driver.  This class gives access to the low-level
	// implementation of metadata.  Implement more specific interfaces
	// on top of this.
	struct metadata {
		enum open_type {
			CREATE,
			OPEN
		};

		metadata(std::string const &dev_path, open_type ot);

		void commit();

		typedef block_manager<>::read_ref read_ref;
		typedef block_manager<>::write_ref write_ref;
		typedef boost::shared_ptr<metadata> ptr;

		tm_ptr tm_;
		superblock sb_;

		checked_space_map::ptr metadata_sm_;
		checked_space_map::ptr data_sm_;
		detail_tree details_;
		dev_tree mappings_top_level_;
		mapping_tree mappings_;
	};
}

//----------------------------------------------------------------

#endif

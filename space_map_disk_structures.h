#ifndef SPACE_MAP_DISK_STRUCTURES_H
#define SPACE_MAP_DISK_STRUCTURES_H

#include "endian.h"
#include "btree.h"

//----------------------------------------------------------------

namespace persistent_data {
	using namespace base;

	namespace sm_disk_detail {
		struct index_entry_disk {
			__le64 blocknr_;
			__le32 nr_free_;
			__le32 none_free_before_;
		} __attribute__ ((packed));

		struct index_entry {
			uint64_t blocknr_;
			uint32_t nr_free_;
			uint32_t none_free_before_;
		};

		struct index_entry_traits {
			typedef index_entry_disk disk_type;
			typedef index_entry value_type;
			typedef NoOpRefCounter<index_entry> ref_counter;

			static void unpack(disk_type const &disk, value_type &value) {
				value.blocknr_ = to_cpu<uint64_t>(disk.blocknr_);
				value.nr_free_ = to_cpu<uint32_t>(disk.nr_free_);
				value.none_free_before_ = to_cpu<uint32_t>(disk.none_free_before_);
			}

			static void pack(value_type const &value, disk_type &disk) {
				disk.blocknr_ = to_disk<__le64>(value.blocknr_);
				disk.nr_free_ = to_disk<__le32>(value.nr_free_);
				disk.none_free_before_ = to_disk<__le32>(value.none_free_before_);
			}
		};

		unsigned const MAX_METADATA_BITMAPS = 255;
		unsigned const ENTRIES_PER_BYTE = 4;

		struct metadata_index {
			__le32 csum_;
			__le32 padding_;
			__le64 blocknr_;

			struct index_entry index[MAX_METADATA_BITMAPS];
		} __attribute__ ((packed));

		struct sm_root_disk {
			__le64 nr_blocks_;
			__le64 nr_allocated_;
			__le64 bitmap_root_;
			__le64 ref_count_root_;
		} __attribute__ ((packed));

		struct sm_root {
			uint64_t nr_blocks_;
			uint64_t nr_allocated_;
			uint64_t bitmap_root_;
			uint64_t ref_count_root_;
		};

		struct sm_root_traits {
			typedef sm_root_disk disk_type;
			typedef sm_root value_type;
			typedef NoOpRefCounter<sm_root> ref_counter;

			static void unpack(disk_type const &disk, value_type &value) {
				value.nr_blocks_ = to_cpu<uint64_t>(disk.nr_blocks_);
				value.nr_allocated_ = to_cpu<uint64_t>(disk.nr_allocated_);
				value.bitmap_root_ = to_cpu<uint64_t>(disk.bitmap_root_);
				value.ref_count_root_ = to_cpu<uint64_t>(disk.ref_count_root_);
			}

			static void pack(value_type const &value, disk_type &disk) {
				disk.nr_blocks_ = to_disk<__le64>(value.nr_blocks_);
				disk.nr_allocated_ = to_disk<__le64>(value.nr_allocated_);
				disk.bitmap_root_ = to_disk<__le64>(value.bitmap_root_);
				disk.ref_count_root_ = to_disk<__le64>(value.ref_count_root_);
			}
		};

		struct bitmap_header {
			__le32 csum;
			__le32 not_used;
			__le64 blocknr;
		} __attribute__ ((packed));
	}
}

//----------------------------------------------------------------

#endif

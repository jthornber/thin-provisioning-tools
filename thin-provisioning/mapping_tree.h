#ifndef MAPPING_TREE_H
#define MAPPING_TREE_H

#include "persistent-data/data-structures/btree.h"
#include "persistent-data/range.h"

//----------------------------------------------------------------

namespace thin_provisioning {
	namespace mapping_tree_detail {
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
			typedef base::le64 disk_type;
			typedef block_time value_type;
			typedef block_time_ref_counter ref_counter;

			static void unpack(disk_type const &disk, value_type &value) {
				uint64_t v = to_cpu<uint64_t>(disk);
				value.block_ = v >> 24;
				value.time_ = v & ((1 << 24) - 1);
			}

			static void pack(value_type const &value, disk_type &disk) {
				uint64_t v = (value.block_ << 24) | value.time_;
				disk = base::to_disk<base::le64>(v);
			}
		};

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
			typedef base::le64 disk_type;
			typedef uint64_t value_type;
			typedef mtree_ref_counter ref_counter;

			static void unpack(disk_type const &disk, value_type &value) {
				value = base::to_cpu<uint64_t>(disk);
			}

			static void pack(value_type const &value, disk_type &disk) {
				disk = base::to_disk<base::le64>(value);
			}
		};
	}

	typedef persistent_data::btree<2, mapping_tree_detail::block_traits> mapping_tree;
	typedef persistent_data::btree<1, mapping_tree_detail::mtree_traits> dev_tree;
	typedef persistent_data::btree<1, mapping_tree_detail::block_traits> single_mapping_tree;
};

//----------------------------------------------------------------

#endif

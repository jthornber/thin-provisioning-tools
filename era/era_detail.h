#ifndef ERA_DETAIL_H
#define ERA_DETAIL_H

#include "base/endian_utils.h"
#include "persistent-data/transaction_manager.h"

//----------------------------------------------------------------

namespace era {
	struct era_detail_disk {
		base::le32 nr_bits;
		base::le64 writeset_root;
	} __attribute__ ((packed));

	struct era_detail {
		era_detail()
			: nr_bits(0),
			  writeset_root(0) {
		}

		uint32_t nr_bits;
		uint64_t writeset_root;
	};

	struct era_detail_ref_counter {
		era_detail_ref_counter(persistent_data::transaction_manager::ptr tm)
			: tm_(tm) {
		}

		void inc(era_detail const &d) {
			tm_->get_sm()->inc(d.writeset_root);
		}

		void dec(persistent_data::block_address b) {
			// I don't think we ever do this in the tools
			throw std::runtime_error("not implemented");
		}

	private:
		persistent_data::transaction_manager::ptr tm_;
	};

	struct era_detail_traits {
		typedef era_detail_disk disk_type;
		typedef era_detail value_type;
		typedef era_detail_ref_counter ref_counter;

		static void unpack(disk_type const &disk, value_type &value);
		static void pack(value_type const &value, disk_type &disk);
	};
}

//----------------------------------------------------------------

#endif

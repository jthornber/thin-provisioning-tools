#ifndef ERA_DETAIL_H
#define ERA_DETAIL_H

#include "base/endian_utils.h"

//----------------------------------------------------------------

namespace era {
	struct era_detail_disk {
		base::le32 nr_bits;
		base::le32 hash_fns_and_probes;
		base::le64 bloom_root;
	} __attribute__ ((packed));

	struct era_detail {
		uint32_t nr_bits;

		uint32_t hash1;
		uint32_t hash2;
		uint32_t nr_probes;

		uint64_t bloom_root;
	};

	struct era_detail_traits {
		typedef era_detail_disk disk_type;
		typedef era_detail value_type;

		static void unpack(disk_type const &disk, value_type &value);
		static void pack(value_type const &value, disk_type &disk);
	};
}

//----------------------------------------------------------------

#endif

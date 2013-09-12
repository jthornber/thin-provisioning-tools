#ifndef CACHE_MAPPING_ARRAY_H
#define CACHE_MAPPING_ARRAY_H

#include "persistent-data/data-structures/array.h"

//----------------------------------------------------------------

namespace caching {
	namespace mapping_array_detail {
		struct mapping {
			uint64_t oblock_;
			uint32_t flags_;
		};

		struct mapping_traits {
			typedef base::le64 disk_type;
			typedef mapping value_type;
			typedef no_op_ref_counter<value_type> ref_counter;

			static void unpack(disk_type const &disk, value_type &value);
			static void pack(value_type const &value, disk_type &disk);
		};

		// FIXME: damage visitor stuff
	}

	typedef persistent_data::array<mapping_array_detail::mapping_traits> mapping_array;
}

//----------------------------------------------------------------

#endif

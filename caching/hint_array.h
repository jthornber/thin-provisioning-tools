#ifndef CACHE_HINT_ARRAY_H
#define CACHE_HINT_ARRAY_H

#include "persistent-data/data-structures/array.h"

#include <string>

//----------------------------------------------------------------

namespace caching {
	namespace hint_array_detail {
		template <uint32_t WIDTH>
		struct hint_traits {
			typedef unsigned char byte;
			typedef byte disk_type[WIDTH];
			typedef std::string value_type;
			typedef no_op_ref_counter<value_type> ref_counter;

			static void unpack(disk_type const &disk, value_type &value);
			static void pack(value_type const &value, disk_type &disk);
		};

		// FIXME: data visitor stuff
	}

//	typedef persistent_data::array<mapping_array_detail::hint_traits> hint_array;
}

//----------------------------------------------------------------

#endif

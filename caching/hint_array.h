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
			typedef vector<byte> value_type;
			typedef no_op_ref_counter<value_type> ref_counter;

			// FIXME: slow copying for now
			static void unpack(disk_type const &disk, value_type &value) {
				for (unsigned byte = 0; byte < WIDTH; byte++)
					value[byte] = disk[byte];
			}

			static void pack(value_type const &value, disk_type &disk) {
				for (unsigned byte = 0; byte < WIDTH; byte++)
					disk[byte] = value[byte];
			}
		};

		// FIXME: data visitor stuff
	}

	class hint_array {
	public:
		typedef boost::shared_ptr<hint_array> ptr;
		typedef typename persistent_data::transaction_manager::ptr tm_ptr;

		hint_array(tm_ptr tm, unsigned width);
		hint_array(tm_ptr tm, unsigned width, block_address root, unsigned nr_entries);

		unsigned get_nr_entries() const;

		void grow(unsigned new_nr_entries, void const *v);

		block_address get_root() const;
		void get_hint(unsigned index, vector<unsigned char> &data) const;
		void set_hint(unsigned index, vector<unsigned char> const &data);

	private:
		unsigned width_;
		boost::shared_ptr<persistent_data::array_base> impl_;
	};
}

//----------------------------------------------------------------

#endif

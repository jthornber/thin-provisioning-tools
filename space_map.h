#ifndef SPACE_MAP_H
#define SPACE_MAP_H

#include "block.h"

#include <boost/shared_ptr.hpp>

//----------------------------------------------------------------

namespace persistent_data {
	typedef uint32_t ref_t;

	class space_map {
	public:
		typedef boost::shared_ptr<space_map> ptr;

		virtual ~space_map() {};

		virtual block_address get_nr_blocks() const = 0;
		virtual block_address get_nr_free() const = 0;
		virtual ref_t get_count(block_address b) const = 0;
		virtual void set_count(block_address b, ref_t c) = 0;
		virtual void commit() = 0;

		virtual void inc(block_address b) = 0;
		virtual void dec(block_address b) = 0;
		virtual block_address new_block() = 0;

		virtual bool count_possibly_greater_than_one(block_address b) const = 0;

		virtual void extend(block_address extra_blocks) = 0;
	};

	class persistent_space_map : public space_map {
	public:
		typedef boost::shared_ptr<persistent_space_map> ptr;

		virtual size_t root_size() = 0;
		virtual void copy_root(void *dest, size_t len) = 0;
	};
}

//----------------------------------------------------------------

#endif

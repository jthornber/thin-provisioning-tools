#include "thin-provisioning/emitter.h"
#include "contrib/tmakatos_mappings_emitter.h"

#include <iostream>
#include <climits>

using namespace std;
using namespace thin_provisioning;

//----------------------------------------------------------------

namespace tmakatos_mappings_emitter {
	template <typename T>
	std::ostream &operator << (ostream &out, boost::optional<T> const &maybe) {
		if (maybe)
			out << *maybe;

		return out;
	}

	//------------------------------------------------
	// mappings binary generator
	//------------------------------------------------
	binary_emitter::binary_emitter(ostream &out): out_(out) {
	}

	void binary_emitter::begin_superblock(string const &uuid,
			      uint64_t time,
			      uint64_t trans_id,
			      boost::optional<uint32_t> flags,
			      boost::optional<uint32_t> version,
			      uint32_t data_block_size,
			      uint64_t nr_data_blocks,
			      boost::optional<uint64_t> metadata_snap) {
	}

	void binary_emitter::end_superblock() {
	}

	void binary_emitter::begin_device(uint32_t dev_id,
			  uint64_t mapped_blocks,
			  uint64_t trans_id,
			  uint64_t creation_time,
			  uint64_t snap_time) {
		prev = 0;

	}

	void binary_emitter::end_device() {
	}

	void binary_emitter::begin_named_mapping(string const &name) {
		assert(false);
	}

	void binary_emitter::end_named_mapping() {
		assert(false);
	}

	void binary_emitter::identifier(string const &name) {
		assert(false);
	}

	void binary_emitter::_range_map(uint64_t data_begin, uint64_t len) {
		for (uint64_t i = 0; i < len; i++) {
			out_.write((const char *)&data_begin, sizeof data_begin);
			if (data_begin != ULLONG_MAX)
				data_begin++;
		}
	}

	void binary_emitter::range_map(uint64_t origin_begin, uint64_t data_begin,
		uint32_t, uint64_t len) {

		assert(len > 0);

		if (prev == 0 && origin_begin >= 1)
			_range_map(ULLONG_MAX, origin_begin - 1);
		else if (origin_begin - prev > 1)
			_range_map(ULLONG_MAX, origin_begin - prev - 1);

		_range_map(data_begin, len);

		prev  = origin_begin + len - 1;
	}

	void binary_emitter::single_map(uint64_t origin_block, uint64_t data_block,
		uint32_t) {

		range_map(origin_block, data_block, 0, 1);
	}
}

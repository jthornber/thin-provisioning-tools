#ifndef _TMAKATOS_EMITTER_H_
#define _TMAKATOS_EMITTER_H_

#include "thin-provisioning/emitter.h"
#include <climits>

using namespace std;
using namespace thin_provisioning;

namespace tmakatos_emitter {
	class binary_emitter : public emitter {
	public:
		binary_emitter(ostream &out);

		void begin_superblock(string const &uuid,
				      uint64_t time,
				      uint64_t trans_id,
				      boost::optional<uint32_t> flags,
				      boost::optional<uint32_t> version,
				      uint32_t data_block_size,
				      uint64_t nr_data_blocks,
				      boost::optional<uint64_t> metadata_snap);

		void end_superblock();

		void begin_device(uint32_t dev_id,
				  uint64_t mapped_blocks,
				  uint64_t trans_id,
				  uint64_t creation_time,
				  uint64_t snap_time);

		void end_device();

		void begin_named_mapping(string const &name);

		void end_named_mapping();

		void identifier(string const &name);

		void range_map(uint64_t origin_begin, uint64_t, uint32_t,
			       uint64_t len);

		void single_map(uint64_t origin_block, uint64_t, uint32_t);

	private:
		ostream &out_;

		/**
		 * The entire virtual block allocation bitmap is segmented into 64-bit
		 * sub-bitmaps (units).
		 */
		uint64_t bitmap;

		/*
		 * Pointer to the current sub-bitmap (unit) that has not yet been
		 * emitted.
		 */
		uint64_t cur;

		/**
		 * Unit (sub-bitmap) size. Must be a power of 2.
		 */
		static const size_t unit = sizeof bitmap * CHAR_BIT;

		void emit_bmp(bool omit_if_zero = false);
	};
}

#endif /* _TMAKATOS_EMITTER_H_ */

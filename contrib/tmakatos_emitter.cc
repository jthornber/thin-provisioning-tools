#include "thin-provisioning/emitter.h"

#include <iostream>
#include <climits>

using namespace std;
using namespace thin_provisioning;

//----------------------------------------------------------------

namespace {
	template <typename T>
	std::ostream &operator << (ostream &out, boost::optional<T> const &maybe) {
		if (maybe)
			out << *maybe;

		return out;
	}

	//------------------------------------------------
	// binary generator
	//------------------------------------------------
	class binary_emitter : public emitter {
	public:
		binary_emitter(ostream &out)
		: out_(out) {
		}

		void begin_superblock(string const &uuid,
				      uint64_t time,
				      uint64_t trans_id,
				      boost::optional<uint32_t> flags,
				      boost::optional<uint32_t> version,
				      uint32_t data_block_size,
				      uint64_t nr_data_blocks,
				      boost::optional<uint64_t> metadata_snap) {
		}

		void end_superblock() {
		}

		void begin_device(uint32_t dev_id,
				  uint64_t mapped_blocks,
				  uint64_t trans_id,
				  uint64_t creation_time,
				  uint64_t snap_time) {
			cur = 0;
			bitmap = 0;
		}

		void end_device() {
			emit_bmp(true);
		}

		void begin_named_mapping(string const &name) { }

		void end_named_mapping() { }

		void identifier(string const &name) { }
		void range_map(uint64_t origin_begin, uint64_t, uint32_t,
			       uint64_t len) {

			uint64_t n = origin_begin / unit;
			uint64_t i;

			assert(n >= cur);
			assert(len > 0);

			/*
			 * Cover the gap between the last emitted unit and the current one.
			 */
			if (n > cur)
				do { emit_bmp(); } while (cur < n);

			/*
			 * Emit partial unit.
			 */
			if (origin_begin & (unit - 1)) {
				const uint64_t j = min(len,
						       (origin_begin & ~(unit - 1)) + unit - origin_begin);
				for (i = origin_begin; i < origin_begin + j; i++)
					bitmap |= 1ULL << (i & (unit - 1));
				if (j == len)
					return;

				emit_bmp();

				len -= j;
				origin_begin = i;
			}

			/*
			 * Emit full units until end.
			 */
			n = (origin_begin + len) / unit;
			while (cur < n) {
				bitmap = ~0;
				emit_bmp();
				len -= unit;
			}
			origin_begin = cur * unit;

			/*
			 * Emit final unit.
			 */
			for (i = origin_begin; i < origin_begin + len; i++)
				bitmap |= 1ULL << (i & (unit - 1));
		}

		void single_map(uint64_t origin_block, uint64_t, uint32_t) {
			range_map(origin_block, 0, 0, 1);
		}

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

		void emit_bmp(bool omit_if_zero = false) {
			if (!bitmap && omit_if_zero)
				out_.write((const char*)&bitmap, sizeof bitmap);
			bitmap = 0;
			cur++;
		}
	};
}

//----------------------------------------------------------------

extern "C" {
	emitter::ptr create_emitter(ostream &out) {
		return emitter::ptr(new binary_emitter(out));
	}
}

//----------------------------------------------------------------

#include "dbg-lib/bitset_block_dumper.h"
#include "dbg-lib/output_formatter.h"
#include "persistent-data/data-structures/array_block.h"
#include "persistent-data/data-structures/simple_traits.h"

using namespace dbg;
using namespace persistent_data;

//----------------------------------------------------------------

namespace {
	class bitset_block_dumper : public dbg::block_dumper {
		typedef array_block<uint64_traits, block_manager::read_ref> rblock;
	public:
		explicit bitset_block_dumper()
			: BITS_PER_ARRAY_ENTRY(64) {
		}

		virtual void show(block_manager::read_ref &rr, ostream &out) {
			rblock b(rr, rc_);
			show_bitset_entries(b, out);
		}

	private:
		void show_bitset_entries(rblock const& b, ostream &out) {
			formatter::ptr f = create_xml_formatter();
			uint32_t nr_entries = b.nr_entries();

			field(*f, "max_entries", b.max_entries());
			field(*f, "nr_entries", nr_entries);
			field(*f, "value_size", b.value_size());

			uint32_t end_pos = b.nr_entries() * BITS_PER_ARRAY_ENTRY;
			std::pair<uint32_t, uint32_t> range = next_set_bits(b, 0);
			for (; range.first < end_pos; range = next_set_bits(b, range.second)) {
				formatter::ptr f2 = create_xml_formatter();
				field(*f2, "begin", range.first);
				field(*f2, "end", range.second);
				f->child("set_bits", f2);
			}

			f->output(out, 0);
		}

		// Returns the range of set bits, starts from the offset.
		pair<uint32_t, uint32_t> next_set_bits(rblock const &b, uint32_t offset) {
			uint32_t end_pos = b.nr_entries() * BITS_PER_ARRAY_ENTRY;
			uint32_t begin = find_first_set(b, offset);

			if (begin == end_pos) // not found
				return make_pair(end_pos, end_pos);

			uint32_t end = find_first_unset(b, begin + 1);
			return make_pair(begin, end);
		}

		// Returns the position (zero-based) of the first bit set
		// in the array block, starts from the offset.
		// Returns the pass-the-end position if not found.
		uint32_t find_first_set(rblock const &b, uint32_t offset) {
			uint32_t entry = offset / BITS_PER_ARRAY_ENTRY;
			uint32_t nr_entries = b.nr_entries();

			if (entry >= nr_entries)
				return entry * BITS_PER_ARRAY_ENTRY;

			uint32_t idx = offset % BITS_PER_ARRAY_ENTRY;
			uint64_t v = b.get(entry++) >> idx;
			while (!v && entry < nr_entries) {
				v = b.get(entry++);
				idx = 0;
			}

			if (!v) // not found
				return entry * BITS_PER_ARRAY_ENTRY;

			return (entry - 1) * BITS_PER_ARRAY_ENTRY + idx + ffsll(static_cast<long long>(v)) - 1;
		}

		// Returns the position (zero-based) of the first zero bit
		// in the array block, starts from the offset.
		// Returns the pass-the-end position if not found.
		// FIXME: improve efficiency
		uint32_t find_first_unset(rblock const& b, uint32_t offset) {
			uint32_t entry = offset / BITS_PER_ARRAY_ENTRY;
			uint32_t nr_entries = b.nr_entries();

			if (entry >= nr_entries)
				return entry * BITS_PER_ARRAY_ENTRY;

			uint32_t idx = offset % BITS_PER_ARRAY_ENTRY;
			uint64_t v = b.get(entry++);
			while (all_bits_set(v, idx) && entry < nr_entries) {
				v = b.get(entry++);
				idx = 0;
			}

			if (all_bits_set(v, idx)) // not found
				return entry * BITS_PER_ARRAY_ENTRY;

			return (entry - 1) * BITS_PER_ARRAY_ENTRY + idx + count_leading_bits(v, idx);
		}

		// Returns true if all the bits beyond the position are set.
		bool all_bits_set(uint64_t v, uint32_t offset) {
			return (v >> offset) == (numeric_limits<uint64_t>::max() >> offset);
		}

		// Counts the number of leading 1's in the given value, starts from the offset
		// FIXME: improve efficiency
		uint32_t count_leading_bits(uint64_t v, uint32_t offset) {
			uint32_t count = 0;

			v >>= offset;
			while (v & 0x1) {
				v >>= 1;
				count++;
			}

			return count;
		}

		block_manager::ptr bm_;
		uint64_traits::ref_counter rc_;

		const uint32_t BITS_PER_ARRAY_ENTRY;
	};
}

//----------------------------------------------------------------

block_dumper::ptr
dbg::create_bitset_block_dumper() {
	return block_dumper::ptr(new bitset_block_dumper());
}

//----------------------------------------------------------------

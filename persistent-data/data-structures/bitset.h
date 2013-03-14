// Copyright (C) 2013 Red Hat, Inc. All rights reserved.
//
// This file is part of the thin-provisioning-tools source.
//
// thin-provisioning-tools is free software: you can redistribute it
// and/or modify it under the terms of the GNU General Public License
// as published by the Free Software Foundation, either version 3 of
// the License, or (at your option) any later version.
//
// thin-provisioning-tools is distributed in the hope that it will be
// useful, but WITHOUT ANY WARRANTY; without even the implied warranty
// of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License along
// with thin-provisioning-tools.  If not, see
// <http://www.gnu.org/licenses/>.

#ifndef BITSET_H
#define BITSET_H

#include "persistent-data/math_utils.h"
#include "persistent-data/data-structures/array.h"

//----------------------------------------------------------------

namespace persistent_data {
	namespace bitset_detail {
		struct bitset_traits {
			typedef base::__le64 disk_type;
			typedef uint64_t value_type;
			typedef no_op_ref_counter<uint64_t> ref_counter;

			static void unpack(disk_type const &disk, value_type &value) {
				value = base::to_cpu<uint64_t>(disk);
			}

			static void pack(value_type const &value, disk_type &disk) {
				disk = base::to_disk<base::__le64>(value);
			}
		};
	}

	class bitset {
	public:
		typedef boost::shared_ptr<bitset> ptr;
		typedef typename persistent_data::transaction_manager::ptr tm_ptr;

		bitset(tm_ptr tm)
		: nr_bits_(0),
		  array_(tm, rc_) {
		}

		bitset(tm_ptr tm, block_address root, unsigned nr_bits)
			: nr_bits_(nr_bits),
			  array_(tm, rc_, root, nr_bits) {
		}

		block_address get_root() const {
			return array_.get_root();
		}

		void grow(unsigned new_nr_bits, bool default_value) {
			pad_last_block(default_value);
			resize_array(new_nr_bits, default_value);
			nr_bits_ = new_nr_bits;
		}

		void destroy();

		// May trigger a flush, so cannot be const
		bool get(unsigned n) {
			check_bounds(n);
			return get_bit(array_.get(word(n)), bit(n));
		}

		void set(unsigned n, bool value) {
			check_bounds(n);
			unsigned w_index = word(n);
			uint64_t w = array_.get(w_index);
			if (value)
				w = set_bit(w, bit(n));
			else
				w = clear_bit(w, bit(n));
			array_.set(w_index, w);
		}

		void flush() {
		}

	private:
		void pad_last_block(bool default_value) {
			// Set defaults in the final word
			if (bit(nr_bits_)) {
				unsigned w_index = word(nr_bits_);
				uint64_t w = array_.get(w_index);

				for (unsigned b = bit(nr_bits_); b < 64; b++)
					if (default_value)
						w = set_bit(w, b);
					else
						w = clear_bit(w, b);

				array_.set(w_index, w);
			}
		}

		void resize_array(unsigned new_nr_bits, bool default_value) {
			unsigned old_nr_words = words_needed(nr_bits_);
			unsigned new_nr_words = words_needed(new_nr_bits);

			if (new_nr_words < old_nr_words)
				throw runtime_error("bitset grow actually asked to shrink");

			if (new_nr_words > old_nr_words)
				array_.grow(new_nr_words, default_value ? ~0 : 0);
		}

		unsigned words_needed(unsigned nr_bits) const {
			return base::div_up<unsigned>(nr_bits, 64u);
		}

		unsigned word(unsigned bit) const {
			return bit / 64;
		}

		uint64_t mask(unsigned bit) const {
			return 1ull << bit;
		}

		bool get_bit(uint64_t w, unsigned bit) const {
			return w & mask(bit);
		}

		uint64_t set_bit(uint64_t w, unsigned bit) const {
			return w | mask(bit);
		}

		uint64_t clear_bit(uint64_t w, unsigned bit) const {
			return w & (~mask(bit));
		}

		unsigned bit(unsigned bit) const {
			return bit % 64;
		}

		// The last word may be only partially full, so we have to
		// do our own bounds checking rather than relying on array
		// to do it.
		void check_bounds(unsigned n) const {
			if (n >= nr_bits_) {
				std::ostringstream str;
				str << "bitset index out of bounds ("
				    << n << " >= " << nr_bits_ << endl;
				throw runtime_error(str.str());
			}
		}

		unsigned nr_bits_;
		no_op_ref_counter<uint64_t> rc_;
		array<bitset_detail::bitset_traits> array_;
	};
}

//----------------------------------------------------------------

#endif

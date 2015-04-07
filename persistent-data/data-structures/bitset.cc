#include "persistent-data/data-structures/array.h"
#include "persistent-data/data-structures/bitset.h"
#include "persistent-data/math_utils.h"

using namespace persistent_data;
using namespace persistent_data::bitset_detail;
using namespace std;

//----------------------------------------------------------------

namespace {
	struct bitset_traits {
		typedef base::le64 disk_type;
		typedef ::uint64_t value_type;
		typedef no_op_ref_counter<uint64_t> ref_counter;

		static void unpack(disk_type const &disk, value_type &value) {
			value = base::to_cpu<uint64_t>(disk);
		}

		static void pack(value_type const &value, disk_type &disk) {
			disk = base::to_disk<base::le64>(value);
		}
	};
}

namespace persistent_data {
	namespace bitset_detail {
		size_t BITS_PER_ULL = 64;

		class bitset_impl {
		public:
			typedef boost::shared_ptr<bitset_impl> ptr;
			typedef persistent_data::transaction_manager::ptr tm_ptr;

			bitset_impl(transaction_manager &tm)
			: nr_bits_(0),
			  array_(tm, rc_) {
			}

			bitset_impl(transaction_manager &tm, block_address root, unsigned nr_bits)
				: nr_bits_(nr_bits),
				  array_(tm, rc_, root, div_up<unsigned>(nr_bits, BITS_PER_ULL)) {
			}

			block_address get_root() const {
				return array_.get_root();
			}

			unsigned get_nr_bits() const {
				return nr_bits_;
			}

			void grow(unsigned new_nr_bits, bool default_value) {
				pad_last_block(default_value);
				resize_array(new_nr_bits, default_value);
				nr_bits_ = new_nr_bits;
			}

			void destroy() {
				throw runtime_error("bitset.destroy() not implemented");
			}

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

			void walk_bitset(bitset_visitor &v) const {
				bit_visitor vv(v, nr_bits_);
				damage_visitor dv(v);
				array_.visit_values(vv, dv);
			}

		private:
			class bit_visitor {
			public:
				bit_visitor(bitset_visitor &v, unsigned nr_bits)
					: v_(v),
					  nr_bits_(nr_bits) {
				}

				void visit(uint32_t word_index, uint64_t word) {
					uint32_t bit_index = word_index * 64;
					for (unsigned bit = 0; bit < 64 && bit_index < nr_bits_; bit++, bit_index++)
						v_.visit(bit_index, !!(word & (1ULL << bit)));
				}

			private:
				bitset_visitor &v_;
				unsigned nr_bits_;
			};

			class damage_visitor {
			public:
				damage_visitor(bitset_visitor &v)
					: v_(v) {
				}

				void visit(array_detail::damage const &d) {
					run<uint32_t> bits(lifted_mult64(d.lost_keys_.begin_),
							   lifted_mult64(d.lost_keys_.end_));
					v_.visit(bits);
				}

			private:
				boost::optional<uint32_t> lifted_mult64(boost::optional<uint32_t> const &m) {
					if (!m)
						return m;

					return boost::optional<uint32_t>(*m * 64);
				}

				bitset_visitor &v_;
			};

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
					    << n << " >= " << nr_bits_ << ")";
					throw runtime_error(str.str());
				}
			}

			unsigned nr_bits_;
			no_op_ref_counter<uint64_t> rc_;
			array<bitset_traits> array_;
		};
	}
}

//----------------------------------------------------------------

persistent_data::bitset::bitset(transaction_manager &tm)
	: impl_(new bitset_impl(tm))
{
}

persistent_data::bitset::bitset(transaction_manager &tm, block_address root, unsigned nr_bits)
	: impl_(new bitset_impl(tm, root, nr_bits))
{
}

block_address
persistent_data::bitset::get_root() const
{
	return impl_->get_root();
}

unsigned
persistent_data::bitset::get_nr_bits() const
{
	return impl_->get_nr_bits();
}

void
persistent_data::bitset::grow(unsigned new_nr_bits, bool default_value)
{
	impl_->grow(new_nr_bits, default_value);
}

void
persistent_data::bitset::destroy()
{
	impl_->destroy();
}

bool
persistent_data::bitset::get(unsigned n)
{
	return impl_->get(n);
}

void
persistent_data::bitset::set(unsigned n, bool value)
{
	impl_->set(n, value);
}

void
persistent_data::bitset::flush()
{
	impl_->flush();
}

void
persistent_data::bitset::walk_bitset(bitset_visitor &v) const
{
	impl_->walk_bitset(v);
}

//----------------------------------------------------------------


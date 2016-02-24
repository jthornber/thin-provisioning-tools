#ifndef BASE_ROLLING_HASH_H
#define BASE_ROLLING_HASH_H

#include <boost/circular_buffer.hpp>
#include <stdint.h>
#include <boost/optional.hpp>

//----------------------------------------------------------------

namespace base {
	namespace hash_detail {
		uint32_t const MULTIPLIER = 4294967291UL;
		uint32_t const SEED = 123;
	}

	class rolling_hash {
	public:
		rolling_hash(unsigned window_size);

		void reset();

		// Returns the current hash
		uint32_t step(uint8_t byte) {
			update_hash(byte);
			return hash_;
		}

		uint32_t get_hash() const {
			return hash_;
		}

	private:
		void update_hash(uint8_t byte) {
			hash_ -= a_to_k_minus_1_ * (buffer_.front() + hash_detail::SEED);
			buffer_.push_back(byte);
			hash_ = (hash_ * a_) + byte + hash_detail::SEED;
		}

		uint32_t a_;
		uint32_t a_to_k_minus_1_;

		uint32_t hash_;
		uint32_t window_size_;

		boost::circular_buffer<uint8_t> buffer_;
	};

	class content_based_hash {
	public:
		content_based_hash(unsigned window_size);
		void reset();

		// Returns a break point relative to the last reset/break.
		boost::optional<unsigned> step(uint8_t byte) {
			boost::optional<unsigned> r;

			rhash_.step(byte);
			len_++;

			if (len_ < min_len_)
				return r;

			if (hit_break(backup_div_))
				backup_break_ = len_;

			if (hit_break(div_)) {
				// found a break
				r = len_;
				len_ = 0;
				backup_break_.reset();

			} else if (len_ >= max_len_) {
				// too big, is there a backup?
				if (backup_break_) {
					len_ -= *backup_break_;
					r = backup_break_;
					backup_break_.reset();

				} else {
					r = len_;
					len_ = 0;
				}
			}

			return r;
		}

	private:
		bool hit_break(uint32_t mask) const {
			uint32_t h = rhash_.get_hash() >> 8;
			return !(h & mask);
		}

		rolling_hash rhash_;

		uint32_t backup_div_;
		uint32_t div_;

		unsigned min_len_;
		unsigned max_len_;

		unsigned len_;
		boost::optional<unsigned> backup_break_;
	};
}

//----------------------------------------------------------------

#endif

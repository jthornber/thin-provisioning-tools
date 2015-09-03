#ifndef BASE_ROLLING_HASH_H
#define BASE_ROLLING_HASH_H

#include <list>
#include <stdint.h>
#include <boost/optional.hpp>

//----------------------------------------------------------------

namespace base {
	class rolling_hash {
	public:
		rolling_hash(unsigned window_size);

		void reset();

		// Returns the current hash
		uint32_t step(uint8_t byte);

		uint32_t get_hash() const;

	private:
		void update_hash(uint8_t byte);

		uint32_t a_;
		uint32_t a_to_k_minus_1_;

		// FIXME: use a ring buffer
		std::list<uint8_t> chars_;

		uint32_t hash_;
		uint32_t window_size_;
	};

	class content_based_hash {
	public:
		content_based_hash(unsigned window_size);
		void reset();

		// Returns a break point relative to the last reset/break.
		boost::optional<unsigned> step(uint8_t byte);

	private:
		bool hit_break(uint32_t div) const;

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

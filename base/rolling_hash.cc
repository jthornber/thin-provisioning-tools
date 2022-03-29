#include "base/rolling_hash.h"

#include <stdexcept>

using namespace base;
using namespace boost;
using namespace hash_detail;
using namespace std;

//----------------------------------------------------------------

rolling_hash::rolling_hash(unsigned window_size)
	: a_(MULTIPLIER),
	  a_to_k_minus_1_(a_),
	  window_size_(window_size),
	  buffer_(window_size) {

	for (unsigned i = 1; i < window_size_ - 1; i++)
		a_to_k_minus_1_ *= a_;

	reset();
}

void
rolling_hash::reset()
{
	// prime with zeroes
	buffer_.clear();

	hash_ = 0;
	for (unsigned i = 0; i < window_size_; i++) {
		hash_ = (hash_ * a_) + SEED;
		buffer_.push_back(0);
	}
}

//--------------------------------

content_based_hash::content_based_hash(unsigned window_size)
{
	if (window_size < 4) {
		throw std::invalid_argument("invalid argument");
	}

	rhash_ = window_size;
	// FIXME: hard coded values
	backup_div_ = (window_size >> 2) - 1;
	div_ = (window_size >> 1) - 1;
	min_len_ = window_size >> 2;
	max_len_ = window_size;
	len_ = 0;
}

void
content_based_hash::reset()
{
	len_ = 0;
	backup_break_.reset();
	rhash_.reset();
}

//----------------------------------------------------------------

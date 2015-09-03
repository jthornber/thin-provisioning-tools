#include "base/rolling_hash.h"

using namespace base;
using namespace boost;
using namespace std;

//----------------------------------------------------------------

namespace {
	uint32_t MULTIPLIER = 4294967291UL;
	uint32_t SEED = 123;
}

rolling_hash::rolling_hash(unsigned window_size)
	: a_(MULTIPLIER),
	  a_to_k_minus_1_(a_),
	  window_size_(window_size) {

	for (unsigned i = 1; i < window_size_ - 1; i++)
		a_to_k_minus_1_ *= a_;

	reset();
}

void
rolling_hash::reset()
{
	// prime with zeroes
	chars_.clear();

	hash_ = 0;
	for (unsigned i = 0; i < window_size_; i++) {
		hash_ = (hash_ * a_) + SEED;
		chars_.push_back(0);
	}
}

uint32_t
rolling_hash::step(uint8_t byte)
{
	update_hash(byte);
	return hash_;
}

uint32_t
rolling_hash::get_hash() const
{
	return hash_;
}

void
rolling_hash::update_hash(uint8_t byte)
{
	hash_ -= a_to_k_minus_1_ * (chars_.front() + SEED);
	chars_.pop_front();
	chars_.push_back(byte);
	hash_ = (hash_ * a_) + byte + SEED;
}

//--------------------------------

content_based_hash::content_based_hash(unsigned window_size)
	: rhash_(window_size),

	  // FIXME: hard coded values
	  backup_div_((window_size / 4) - 1),
	  div_((window_size / 2) - 1),
	  min_len_(window_size / 8),
	  max_len_(window_size),
	  len_(0)
{
}

void
content_based_hash::reset()
{
	len_ = 0;
	backup_break_.reset();
	rhash_.reset();
}

optional<unsigned>
content_based_hash::step(uint8_t byte)
{
#if 0
	optional<unsigned> r;

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
#else
	optional<unsigned> r;

	rhash_.step(byte);
	len_++;

	if (len_ < min_len_)
		return r;

	if (hit_break(div_)) {
		// found a break
		r = len_;
		len_ = 0;
		backup_break_.reset();

	} else if (len_ >= max_len_) {
		r = len_;
		len_ = 0;
	}

	return r;
#endif
}

bool
content_based_hash::hit_break(uint32_t mask) const
{
	uint32_t h = rhash_.get_hash() >> 8;
	return !(h & mask);
}

//----------------------------------------------------------------

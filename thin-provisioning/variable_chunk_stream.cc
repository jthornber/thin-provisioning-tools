#include "thin-provisioning/variable_chunk_stream.h"

using namespace boost;
using namespace std;
using namespace thin_provisioning;

//----------------------------------------------------------------

variable_chunk_stream::variable_chunk_stream(chunk_stream &stream, unsigned window_size)
	: index_(0),
	  h_(window_size),
	  stream_(stream),
	  big_chunk_(0) {
	next_big_chunk();
}

variable_chunk_stream::~variable_chunk_stream()
{
	put_big_chunk();
}

void
variable_chunk_stream::rewind()
{
	// FIXME: not complete
	index_ = 0;
	stream_.rewind();
	h_.reset();
}

bool
variable_chunk_stream::next(bcache::block_address count)
{
	while (count--) {
		index_++;
		advance_one();
	}

	return !eof();
}

bool
variable_chunk_stream::eof() const
{
	return stream_.eof();
}

bcache::block_address
variable_chunk_stream::index() const
{
	return index_;
}

chunk const &
variable_chunk_stream::get()
{
	assert(big_chunk_);

	little_chunk_.len_ = little_e_ - little_b_;
	little_chunk_.offset_ = big_chunk_->offset_ + little_chunk_.len_;

	little_chunk_.mem_.clear();
	little_chunk_.mem_.push_back(mem(little_b_, little_e_));

	return little_chunk_;
}

void
variable_chunk_stream::put(chunk const &c)
{
	// noop
}

bool
variable_chunk_stream::next_big_chunk()
{
	put_big_chunk();

	if (!stream_.next())
		return false;

	big_chunk_ = &stream_.get();
	little_b_ = little_e_ = last_hashed_ = big_chunk_->mem_.front().begin;
	h_.reset();

	return true;
}

bool
variable_chunk_stream::advance_one()
{
	uint8_t *big_e;

	assert(big_chunk_);

	big_e = big_chunk_->mem_.front().end;
	little_b_ = little_e_;
	little_e_ = last_hashed_;

	if (little_b_ == big_e) {
		if (next_big_chunk())
			big_e = big_chunk_->mem_.front().end;
		else
			return false;
	}

	assert(little_e_ >= big_chunk_->mem_.front().begin);
	assert(little_b_ >= big_chunk_->mem_.front().begin);
	assert(little_e_ <= big_e);
	assert(little_b_ <= big_e);


	while (little_e_ != big_e) {
		optional<unsigned> maybe_break = h_.step(*little_e_);
		little_e_++;

		if (maybe_break) {
			// The break is not neccessarily at the current
			// byte.
			last_hashed_ = little_e_;
			little_e_ = little_b_ + *maybe_break;
			break;
		}
	}

	if (little_e_ == big_e)
		last_hashed_ = little_e_;

	assert(little_e_ >= big_chunk_->mem_.front().begin);
	assert(little_b_ >= big_chunk_->mem_.front().begin);
	assert(little_e_ <= big_e);
	assert(little_b_ <= big_e);

	return true;
}

void
variable_chunk_stream::put_big_chunk()
{
	if (big_chunk_)
		stream_.put(*big_chunk_);

	big_chunk_ = 0;
}

//----------------------------------------------------------------

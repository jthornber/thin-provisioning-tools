#include "thin-provisioning/fixed_chunk_stream.h"

using namespace thin_provisioning;

//----------------------------------------------------------------

fixed_chunk_stream::fixed_chunk_stream(chunk_stream &stream, unsigned chunk_size)
	: index_(0),
	  stream_(stream),
	  chunk_size_(chunk_size),
	  big_chunk_(0) {
	next_big_chunk();
}

fixed_chunk_stream::~fixed_chunk_stream()
{
	put_big_chunk();
}

bcache::block_address
fixed_chunk_stream::size() const
{
	return stream_.size();
}

void
fixed_chunk_stream::rewind()
{
	// FIXME: not complete
	index_ = 0;
	stream_.rewind();
}

bool
fixed_chunk_stream::next(bcache::block_address count)
{
	while (count--) {
		index_++;
		advance_one();
	}

	return !eof();
}

bool
fixed_chunk_stream::eof() const
{
	return stream_.eof();
}

chunk const &
fixed_chunk_stream::get()
{
	assert(big_chunk_);

	little_chunk_.len_ = little_e_ - little_b_;
	little_chunk_.offset_ = big_chunk_->offset_ + little_chunk_.len_;

	little_chunk_.mem_.begin = little_b_;
	little_chunk_.mem_.end = little_e_;

	return little_chunk_;
}

void
fixed_chunk_stream::put(chunk const &c)
{
	// noop
}

bool
fixed_chunk_stream::next_big_chunk()
{
	put_big_chunk();

	if (!stream_.next())
		return false;

	big_chunk_ = &stream_.get();
	little_b_ = little_e_ = last_hashed_ = big_chunk_->mem_.begin;

	return true;
}

bool
fixed_chunk_stream::advance_one()
{
	uint8_t *big_e;

	big_e = big_chunk_->mem_.end;
	little_b_ = little_e_;

	if (little_b_ >= big_e) {
		if (next_big_chunk())
			big_e = big_chunk_->mem_.end;
		else
			return false;
	}

	little_e_ += chunk_size_;
	return true;
}

void
fixed_chunk_stream::put_big_chunk()
{
	if (big_chunk_)
		stream_.put(*big_chunk_);

	big_chunk_ = 0;
}

//----------------------------------------------------------------

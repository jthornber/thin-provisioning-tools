#include "xdr/xdr.h"

#include <string.h>
#include <stdexcept>

using namespace xdr;
using namespace std;

//----------------------------------------------------------------

xdr_write_buffer::xdr_write_buffer(uint8_t *begin, uint8_t *end)
	: begin_(begin),
	  end_(end),
	  current_(begin),
	  padding_(0)
{
}

void
xdr_write_buffer::push_bytes(uint8_t const *begin, uint8_t const *end)
{
	uint32_t len = end - begin;
	unsigned padding = (4 - (len % 4)) % 4;

	if (len > end_ - current_)
		throw runtime_error("xdr buffer out of space");

	memcpy(current_, begin, len);
	current_ += len;
	memcpy(current_, &padding_, padding);
	current_ += padding;
}

uint8_t const *
xdr_write_buffer::begin() const
{
	return begin_;
}

uint8_t const *
xdr_write_buffer::end() const
{
	return current_;
}

void
xdr_write_buffer::encode(bool n)
{
	uint32_t n_ = n ? 1u : 0u;
	push_word(n_);
}

void
xdr_write_buffer::encode(int32_t n)
{
	push_word(n);
}

void
xdr_write_buffer::encode(uint32_t n)
{
	push_word(n);
}

void
xdr_write_buffer::encode(int64_t n)
{
	push_hyper(n);
}

void
xdr_write_buffer::encode(uint64_t n)
{
	push_hyper(n);
}


void
xdr_write_buffer::encode_fixed(uint32_t len, xdr_opaque const &data)
{
	if (data.size() != len)
		throw runtime_error("fixed length opaque not of expected length");

	push_bytes(data.begin(), data.end());
}

void
xdr_write_buffer::encode_variable(uint32_t max, xdr_opaque const &data)
{
	uint32_t len = data.size();
	if (len > max)
		throw runtime_error("opaque too long");

	push_word(len);
	push_bytes(data.begin(), data.end());
}

void
xdr_write_buffer::encode(uint32_t max, std::string const &str)
{
	uint32_t len = str.size();
	if (len > max)
		throw runtime_error("string too long");

	push_word(len);
	// FIXME: use str.copy() ?
	push_bytes(reinterpret_cast<uint8_t const *>(str.data()),
		   reinterpret_cast<uint8_t const *>(str.data() + len));
}

void
xdr_write_buffer::push_word(uint32_t)
{

}

void
xdr_write_buffer::push_hyper(uint64_t)
{

}

//--------------------------------

xdr_read_buffer::xdr_read_buffer(uint8_t const *begin, uint8_t const *end)
	: begin_(begin_),
	  end_(end),
	  current_(begin_)
{
}

void
xdr_read_buffer::pop_bytes(uint8_t *begin, uint8_t *end)
{
	uint32_t len = end - begin;
	if (len % 4)
		throw runtime_error("attempt to pop non multiple of 4 nr bytes to xdr buffer");

	if (len > end_ - current_)
		throw runtime_error("xdr buffer out of space");

	memcpy(begin, current_, len);
	current_ += len;
}

void
xdr_read_buffer::decode(bool &n)
{
	uint32_t w = pop_word();
	n = w;
}

void
xdr_read_buffer::decode(int32_t &n)
{
	uint32_t n_ = pop_word();
	n = *reinterpret_cast<int32_t *>(&n_);
}

void
xdr_read_buffer::decode(uint32_t &n)
{
	n = pop_word();
}

void
xdr_read_buffer::decode(int64_t &n)
{
	uint64_t n_ = pop_hyper();
	n = *reinterpret_cast<int64_t *>(&n_);
}

void
xdr_read_buffer::decode(uint64_t &n)
{
	n = pop_hyper();
}

void
xdr_read_buffer::decode_fixed(uint32_t len, xdr_opaque &data)
{
	data.resize(len);
	pop_bytes(data.begin(), data.end());
}

void
xdr_read_buffer::decode_variable(uint32_t max, xdr_opaque &data)
{
	uint32_t len = pop_word();

	if (len > max)
		throw runtime_error("opaque too long");

	data.resize(len);
	pop_bytes(data.begin(), data.end());
}

void
xdr_read_buffer::decode(uint32_t max, std::string &str)
{
	uint32_t len = pop_word();

	if (len > max)
		throw runtime_error("string too long");

	str.reserve(len);
	// FIXME: slow
	for (unsigned i = 0; i < len; i++)
		str[i] = current_[i];
	inc_current(len);
}

void
xdr_read_buffer::inc_current(size_t len)
{

}

//----------------------------------------------------------------


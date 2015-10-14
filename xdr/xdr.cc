#include "xdr/xdr.h"

#include <arpa/inet.h>
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
xdr_write_buffer::push_word(uint32_t n)
{
	uint32_t encoded = htonl(n);
	if (current_ + sizeof(uint32_t) > end_)
		throw runtime_error("insufficient space");
	memcpy(current_, &encoded, sizeof(uint32_t));
	current_ += sizeof(uint32_t);
}

void
xdr_write_buffer::push_hyper(uint64_t n)
{
	uint32_t low = n & ((1ull << 32) - 1);
	uint32_t high = n >> 32;

	push_word(high);
	push_word(low);
}

//--------------------------------

void
xdr::encode(xdr_write_buffer &buf, bool n)
{
	uint32_t n_ = n ? 1u : 0u;
	buf.push_word(n_);
}

void
xdr::encode(xdr_write_buffer &buf, int32_t n)
{
	buf.push_word(n);
}

void
xdr::encode(xdr_write_buffer &buf, uint32_t n)
{
	buf.push_word(n);
}

void
xdr::encode(xdr_write_buffer &buf, int64_t n)
{
	buf.push_hyper(n);
}

void
xdr::encode(xdr_write_buffer &buf, uint64_t n)
{
	buf.push_hyper(n);
}

void
xdr::encode_fixed(xdr_write_buffer &buf, uint32_t len, xdr_opaque const &data)
{
	if (data.size() != len)
		throw runtime_error("fixed length opaque not of expected length");

	buf.push_bytes(data.begin(), data.end());
}

void
xdr::encode_variable(xdr_write_buffer &buf, uint32_t max, xdr_opaque const &data)
{
	uint32_t len = data.size();
	if (len > max)
		throw runtime_error("opaque too long");

	buf.push_word(len);
	buf.push_bytes(data.begin(), data.end());
}

void
xdr::encode(xdr_write_buffer &buf, uint32_t max, std::string const &str)
{
	uint32_t len = str.size();
	if (len > max)
		throw runtime_error("string too long");

	buf.push_word(len);
	// FIXME: use str.copy() ?
	buf.push_bytes(reinterpret_cast<uint8_t const *>(str.data()),
		       reinterpret_cast<uint8_t const *>(str.data() + len));
}

//--------------------------------

xdr_read_buffer::xdr_read_buffer(uint8_t const *begin, uint8_t const *end)
	: begin_(begin),
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

	ensure_data(len);
	memcpy(begin, current_, len);
	current_ += len;
}

void
xdr_read_buffer::pop_bytes_no_copy(uint32_t len, uint8_t const **begin, uint8_t const **end)
{
	if (current_ + len)
		throw runtime_error("insufficient data");

	*begin = current_;
	*end = *begin + len;
	current_ += len;
}

uint32_t
xdr_read_buffer::pop_word()
{
	uint32_t r;

	ensure_data(sizeof(r));
	memcpy(&r, current_, sizeof(r));
	inc_current(sizeof(uint32_t));
	return ntohl(r);
}

uint64_t
xdr_read_buffer::pop_hyper()
{
	uint64_t r = 0;

	uint64_t high = pop_word();
	uint64_t low = pop_word();

	r = (high << 32) | low;
	return r;
}

void
xdr_read_buffer::inc_current(size_t len)
{
	ensure_data(len);
	current_ += len;
}

void
xdr_read_buffer::ensure_data(size_t len) const
{
	if (current_ + len > end_)
		throw runtime_error("insufficient data");
}

//--------------------------------

void
xdr::decode(xdr_read_buffer &buf, bool &n)
{
	n = buf.pop_word();
}

void
xdr::decode(xdr_read_buffer &buf, int32_t &n)
{
	uint32_t n_ = buf.pop_word();
	n = *reinterpret_cast<int32_t *>(&n_);
}

void
xdr::decode(xdr_read_buffer &buf, uint32_t &n)
{
	n = buf.pop_word();
}

void
xdr::decode(xdr_read_buffer &buf, int64_t &n)
{
	uint64_t n_ = buf.pop_hyper();
	n = *reinterpret_cast<int64_t *>(&n_);
}

void
xdr::decode(xdr_read_buffer &buf, uint64_t &n)
{
	n = buf.pop_hyper();
}

void
xdr::decode_fixed(xdr_read_buffer &buf, uint32_t len, xdr_opaque &data)
{
	uint8_t const *b, *e;

	buf.pop_bytes_no_copy(len, &b, &e);
	data = xdr_opaque(b, e);
}

void
xdr::decode_variable(xdr_read_buffer &buf, uint32_t max, xdr_opaque &data)
{
	uint32_t len = buf.pop_word();

	if (len > max)
		throw runtime_error("opaque too long");

	uint8_t const *b, *e;
	buf.pop_bytes_no_copy(len, &b, &e);
	data = xdr_opaque(b, e);
}

void
xdr::decode(xdr_read_buffer &buf, uint32_t max, std::string &str)
{
	uint32_t len = buf.pop_word();

	if (len > max)
		throw runtime_error("string too long");

	str.reserve(len);
	buf.pop_bytes(reinterpret_cast<uint8_t *>(&str[0]),
	 		reinterpret_cast<uint8_t *>(&str[len]));
}

//----------------------------------------------------------------

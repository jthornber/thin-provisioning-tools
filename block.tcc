#include "block.h"

#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include <boost/bind.hpp>
#include <iostream>
#include <stdexcept>

using namespace boost;
using namespace persistent_data;
using namespace std;

//----------------------------------------------------------------

template <uint32_t BlockSize>
block_manager<BlockSize>::read_ref::read_ref(typename block_manager::block::ptr b)
	: block_(b)
{
}

template <uint32_t BlockSize>
block_address
block_manager<BlockSize>::read_ref::get_location() const
{
	return block_->location_;
}

template <uint32_t BlockSize>
typename block_manager<BlockSize>::const_buffer &
block_manager<BlockSize>::read_ref::data() const
{
	return block_->data_;
}

template <uint32_t BlockSize>
block_manager<BlockSize>::write_ref::write_ref(typename block_manager::block::ptr b)
	: read_ref(b)
{
}

template <uint32_t BlockSize>
typename block_manager<BlockSize>::buffer &
block_manager<BlockSize>::write_ref::data()
{
	return read_ref::block_->data_;
}

//----------------------------------------------------------------

template <uint32_t BlockSize>
block_manager<BlockSize>::block_manager(std::string const &path, block_address nr_blocks)
	: nr_blocks_(nr_blocks)
{
	fd_ = ::open(path.c_str(), O_RDWR | O_CREAT, 0666);
	if (fd_ < 0)
		throw std::runtime_error("couldn't open file");
}

template <uint32_t BlockSize>
block_manager<BlockSize>::~block_manager()
{
	::close(fd_);
}

template <uint32_t BlockSize>
typename block_manager<BlockSize>::read_ref
block_manager<BlockSize>::read_lock(block_address location) const
{
	check(location);

	typename block::ptr b(new block(location));
	read_block(*b);
	return read_ref(b);
}

template <uint32_t BlockSize>
optional<typename block_manager<BlockSize>::read_ref>
block_manager<BlockSize>::read_try_lock(block_address location) const
{
	return read_lock(location);
}

template <uint32_t BlockSize>
typename block_manager<BlockSize>::write_ref
block_manager<BlockSize>::write_lock(block_address location)
{
	check(location);

	typename block::ptr b(new block(location), bind(&block_manager::write_and_release, this, _1));
	read_block(*b);
	return write_ref(b);
}

template <uint32_t BlockSize>
typename block_manager<BlockSize>::write_ref
block_manager<BlockSize>::write_lock_zero(block_address location)
{
	check(location);

	typename block::ptr b(new block(location), bind(&block_manager<BlockSize>::write_and_release, this, _1));
	zero_block(*b);
	return write_ref(b);
}

template <uint32_t BlockSize>
typename block_manager<BlockSize>::read_ref
block_manager<BlockSize>::read_lock(block_address location,
				    typename block_manager<BlockSize>::validator::ptr const &v) const
{
	check(location);

	typename block::ptr b(new block(location, v));
	read_block(*b);
	return read_ref(b);
}

template <uint32_t BlockSize>
optional<typename block_manager<BlockSize>::read_ref>
block_manager<BlockSize>::read_try_lock(block_address location,
					typename block_manager<BlockSize>::validator::ptr const &v) const
{
	return read_lock(location, v);
}

template <uint32_t BlockSize>
typename block_manager<BlockSize>::write_ref
block_manager<BlockSize>::write_lock(block_address location,
				     typename block_manager<BlockSize>::validator::ptr const &v)
{
	check(location);

	typename block::ptr b(new block(location, v),
			      bind(&block_manager::write_and_release, this, _1));
	read_block(*b);
	return write_ref(b);
}

template <uint32_t BlockSize>
typename block_manager<BlockSize>::write_ref
block_manager<BlockSize>::write_lock_zero(block_address location,
					  typename block_manager<BlockSize>::validator::ptr const &v)
{
	check(location);

	typename block::ptr b(new block(location, v),
			      bind(&block_manager::write_and_release, this, _1));
	zero_block(*b);
	return write_ref(b);
}

template <uint32_t BlockSize>
void
block_manager<BlockSize>::flush(block_manager<BlockSize>::write_ref super_block)
{
	// FIXME: the caller still holds the write_ref, so the superblock
	// will get written twice
	write_block(super_block);
	::fsync(fd_);
}

template <uint32_t BlockSize>
void
block_manager<BlockSize>::read_block(block &b) const
{
	off_t r;
	r = ::lseek(fd_, BlockSize * b.location_, SEEK_SET);
	if (r == (off_t) -1)
		throw std::runtime_error("lseek failed");

	ssize_t n;
	size_t remaining = BlockSize;
	unsigned char *buf = b.data_;
	do {
		n = ::read(fd_, buf, remaining);
		if (n > 0) {
			remaining -= n;
			buf += n;
		}
	} while (remaining && ((n > 0) || (n == EINTR) || (n == EAGAIN)));

	if (n < 0)
		throw std::runtime_error("read failed");

	b.initialised_ = true;
}

template <uint32_t BlockSize>
void
block_manager<BlockSize>::write_block(block const &b)
{
	off_t r;
	r = ::lseek(fd_, BlockSize * b.location_, SEEK_SET);
	if (r == (off_t) -1)
		throw std::runtime_error("lseek failed");

	ssize_t n;
	size_t remaining = BlockSize;
	unsigned char const *buf = b.data_;
	do {
		n = ::write(fd_, buf, remaining);
		if (n > 0) {
			remaining -= n;
			buf += n;
		}
	} while (remaining && ((n > 0) || (n == EINTR) || (n == EAGAIN)));

	if (n < 0)
		throw std::runtime_error("write failed");
}

template <uint32_t BlockSize>
void
block_manager<BlockSize>::zero_block(block &b)
{
	memset(b.data_, 0, BlockSize);
	b.initialised_ = true;
}

template <uint32_t BlockSize>
void
block_manager<BlockSize>::write_and_release(block *b)
{
	if (b->initialised_) {
		if (b->validator_)
			(*b->validator_)->prepare(*b);

		write_block(*b);
	}

	delete b;
}

template <uint32_t BlockSize>
void
block_manager<BlockSize>::check(block_address b) const
{
	if (b >= nr_blocks_)
		throw std::runtime_error("block address out of bounds");
}

//----------------------------------------------------------------

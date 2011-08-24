#include "block.h"

#include <errno.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include <boost/bind.hpp>
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
	: nr_blocks_(nr_blocks),
	  lock_count_(0),
	  superblock_count_(0),
	  ordinary_count_(0)
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

	buffer buf;
	read_buffer(location, buf);
	register_lock(location, READ_LOCK);
	return read_ref(
		typename block::ptr(
			new block(location, buf, lock_count_, ordinary_count_),
			bind(&block_manager::read_release, this, _1)));
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

	buffer buf;
	read_buffer(location, buf);
	register_lock(location, WRITE_LOCK);
	return write_ref(
		typename block::ptr(
			new block(location, buf, lock_count_, ordinary_count_),
			bind(&block_manager::write_release, this, _1)));
}

template <uint32_t BlockSize>
typename block_manager<BlockSize>::write_ref
block_manager<BlockSize>::write_lock_zero(block_address location)
{
	check(location);

	buffer buf;
	zero_buffer(buf);
	register_lock(location, WRITE_LOCK);
	typename block::ptr b(new block(location, buf, lock_count_, ordinary_count_),
			      bind(&block_manager<BlockSize>::write_release, this, _1));

	return write_ref(b);
}

template <uint32_t BlockSize>
typename block_manager<BlockSize>::read_ref
block_manager<BlockSize>::read_lock(block_address location,
				    typename block_manager<BlockSize>::validator::ptr v) const
{
	check(location);

	buffer buf;
	read_buffer(location, buf);
	typename block::ptr b(new block(location, buf, lock_count_, ordinary_count_, false, v),
			      bind(&block_manager::read_release, this, _1));
	register_lock(location, READ_LOCK);
	return read_ref(b);
}

template <uint32_t BlockSize>
optional<typename block_manager<BlockSize>::read_ref>
block_manager<BlockSize>::read_try_lock(block_address location,
					typename block_manager<BlockSize>::validator::ptr v) const
{
	return read_lock(location, v);
}

template <uint32_t BlockSize>
typename block_manager<BlockSize>::write_ref
block_manager<BlockSize>::write_lock(block_address location,
				     typename block_manager<BlockSize>::validator::ptr v)
{
	check(location);

	buffer buf;
	read_buffer(location, buf);
	typename block::ptr b(new block(location, buf, lock_count_, ordinary_count_, false, v),
			      bind(&block_manager::write_release, this, _1));
	register_lock(location, WRITE_LOCK);
	return write_ref(b);
}

template <uint32_t BlockSize>
typename block_manager<BlockSize>::write_ref
block_manager<BlockSize>::write_lock_zero(block_address location,
					  typename block_manager<BlockSize>::validator::ptr v)
{
	check(location);

	buffer buf;
	zero_buffer(buf);
	typename block::ptr b(new block(location, buf, lock_count_, ordinary_count_, false, v),
			      bind(&block_manager::write_release, this, _1));
	register_lock(location, WRITE_LOCK);
	return write_ref(b);
}

template <uint32_t BlockSize>
typename block_manager<BlockSize>::write_ref
block_manager<BlockSize>::superblock(block_address location)
{
	check(location);

	if (superblock_count_ > 0)
		throw runtime_error("already have superblock");

	buffer buf;
	read_buffer(location, buf);
	typename block::ptr b(new block(location, buf, lock_count_, superblock_count_, true),
			      bind(&block_manager::write_release, this, _1));
	register_lock(location, WRITE_LOCK);
	return write_ref(b);
}

template <uint32_t BlockSize>
typename block_manager<BlockSize>::write_ref
block_manager<BlockSize>::superblock_zero(block_address location)
{
	check(location);

	if (superblock_count_ > 0)
		throw runtime_error("already have superblock");

	buffer buf;
	zero_buffer(buf);
	typename block::ptr b(new block(location, buf, lock_count_, superblock_count_, true),
			      bind(&block_manager::write_release, this, _1));
	register_lock(location, WRITE_LOCK);
	return write_ref(b);
}

template <uint32_t BlockSize>
typename block_manager<BlockSize>::write_ref
block_manager<BlockSize>::superblock(block_address location,
				     typename block_manager<BlockSize>::validator::ptr v)
{
	if (superblock_count_ > 0)
		throw runtime_error("already have superblock");

	check(location);

	buffer buf;
	read_buffer(location, buf);
	typename block::ptr b(new block(location, buf, lock_count_, superblock_count_, true, v),
			      bind(&block_manager::write_release, this, _1));
	register_lock(location, WRITE_LOCK);
	return write_ref(b);
}

template <uint32_t BlockSize>
typename block_manager<BlockSize>::write_ref
block_manager<BlockSize>::superblock_zero(block_address location,
					  typename block_manager<BlockSize>::validator::ptr v)
{
	if (superblock_count_ > 0)
		throw runtime_error("already have superblock");

	check(location);

	buffer buf;
	zero_buffer(buf);
	typename block::ptr b(new block(location, buf, lock_count_, superblock_count_, true, v),
			      bind(&block_manager::write_release, this, _1));
	register_lock(location, WRITE_LOCK);
	return write_ref(b);
}

template <uint32_t BlockSize>
void
block_manager<BlockSize>::flush()
{
	if (lock_count_ > 0)
		throw runtime_error("asked to flush while locks are still held");
	::fsync(fd_);
}

template <uint32_t BlockSize>
void
block_manager<BlockSize>::read_buffer(block_address b, block_manager<BlockSize>::buffer &buffer) const
{
	off_t r;
	r = ::lseek(fd_, BlockSize * b, SEEK_SET);
	if (r == (off_t) -1)
		throw std::runtime_error("lseek failed");

	ssize_t n;
	size_t remaining = BlockSize;
	unsigned char *buf = buffer;
	do {
		n = ::read(fd_, buf, remaining);
		if (n > 0) {
			remaining -= n;
			buf += n;
		}
	} while (remaining && ((n > 0) || (n == EINTR) || (n == EAGAIN)));

	if (n < 0)
		throw std::runtime_error("read failed");
}

template <uint32_t BlockSize>
void
block_manager<BlockSize>::write_buffer(block_address b, block_manager<BlockSize>::const_buffer &buffer)
{
	off_t r;
	r = ::lseek(fd_, BlockSize * b, SEEK_SET);
	if (r == (off_t) -1)
		throw std::runtime_error("lseek failed");

	ssize_t n;
	size_t remaining = BlockSize;
	unsigned char const *buf = buffer;
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
block_manager<BlockSize>::zero_buffer(block_manager<BlockSize>::buffer &buffer) const
{
	::memset(buffer, 0, BlockSize);
}

// FIXME: we don't need this anymore
template <uint32_t BlockSize>
void
block_manager<BlockSize>::read_release(block *b) const
{
	unregister_lock(b->location_, READ_LOCK);
	delete b;
}

template <uint32_t BlockSize>
void
block_manager<BlockSize>::write_release(block *b)
{
	if (b->is_superblock_) {
		if (lock_count_ != 1)
			throw runtime_error("superblock isn't the last block");
	}

	if (b->validator_)
		(*b->validator_)->prepare(*b);

	write_buffer(b->location_, b->data_);
	unregister_lock(b->location_, WRITE_LOCK);
	delete b;
}

template <uint32_t BlockSize>
void
block_manager<BlockSize>::check(block_address b) const
{
	if (b >= nr_blocks_)
		throw std::runtime_error("block address out of bounds");
}

template <uint32_t BlockSize>
block_address
block_manager<BlockSize>::get_nr_blocks() const
{
	return nr_blocks_;
}

// FIXME: how do we unregister if block construction throws?
template <uint32_t BlockSize>
void
block_manager<BlockSize>::register_lock(block_address b, lock_type t) const
{
	typename held_map::iterator it = held_locks_.find(b);
	if (it == held_locks_.end())
		held_locks_.insert(make_pair(b, make_pair(t, 1)));
	else {
		if (it->second.first != t)
			throw std::runtime_error("lock type mismatch when locking");

		if (it->second.first == WRITE_LOCK)
			throw std::runtime_error("cannot hold concurrent write locks");

		it->second.second++;
	}
}

template <uint32_t BlockSize>
void
block_manager<BlockSize>::unregister_lock(block_address b, lock_type t) const
{
	typename held_map::iterator it = held_locks_.find(b);
	if (it == held_locks_.end())
		throw std::runtime_error("lock not held");

	if (it->second.first != t)
		throw std::runtime_error("lock type mismatch when unlocking");

	it->second.second--;
	if (it->second.second == 0)
		held_locks_.erase(it);
}

//----------------------------------------------------------------

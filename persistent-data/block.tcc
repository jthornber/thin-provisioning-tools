// Copyright (C) 2011 Red Hat, Inc. All rights reserved.
//
// This file is part of the thin-provisioning-tools source.
//
// thin-provisioning-tools is free software: you can redistribute it
// and/or modify it under the terms of the GNU General Public License
// as published by the Free Software Foundation, either version 3 of
// the License, or (at your option) any later version.
//
// thin-provisioning-tools is distributed in the hope that it will be
// useful, but WITHOUT ANY WARRANTY; without even the implied warranty
// of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License along
// with thin-provisioning-tools.  If not, see
// <http://www.gnu.org/licenses/>.

#include "block.h"

#include <errno.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include <boost/bind.hpp>
#include <stdexcept>
#include <sstream>

using namespace boost;
using namespace persistent_data;
using namespace std;

//----------------------------------------------------------------

template <uint32_t BlockSize>
block_io<BlockSize>::block_io(std::string const &path, block_address nr_blocks, bool writeable)
	: nr_blocks_(nr_blocks),
	  writeable_(writeable)
{
	// fd_ = ::open(path.c_str(), writeable ? (O_RDWR | O_CREAT) : O_RDONLY, 0666);
	fd_ = ::open(path.c_str(), O_DIRECT | O_SYNC | (writeable ? O_RDWR : O_RDONLY), 0666);
	if (fd_ < 0)
		throw std::runtime_error("couldn't open file");
}

template <uint32_t BlockSize>
block_io<BlockSize>::~block_io()
{
	::close(fd_);
}

template <uint32_t BlockSize>
void
block_io<BlockSize>::read_buffer(block_address location, buffer<BlockSize> &buffer) const
{
	off_t r;
	r = ::lseek(fd_, BlockSize * location, SEEK_SET);
	if (r == (off_t) -1)
		throw std::runtime_error("lseek failed");

	ssize_t n;
	size_t remaining = BlockSize;
	unsigned char *buf = buffer.raw();
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
block_io<BlockSize>::write_buffer(block_address location, buffer<BlockSize> const &buffer)
{
	off_t r;
	r = ::lseek(fd_, BlockSize * location, SEEK_SET);
	if (r == (off_t) -1)
		throw std::runtime_error("lseek failed");

	ssize_t n;
	size_t remaining = BlockSize;
	unsigned char const *buf = buffer.raw();
	do {
		n = ::write(fd_, buf, remaining);
		if (n > 0) {
			remaining -= n;
			buf += n;
		}
	} while (remaining && ((n > 0) || (n == EINTR) || (n == EAGAIN)));

	if (n < 0) {
		std::ostringstream out;
		out << "write failed to block " << location
		    << ", block size = " << BlockSize
		    << ", remaining = " << remaining
		    << ", n = " << n
		    << ", errno = " << errno
		    << ", fd_ = " << fd_
		    << std::endl;
		throw std::runtime_error(out.str());
	}
}

//----------------------------------------------------------------

template <uint32_t BlockSize>
block_manager<BlockSize>::block::block(typename block_io<BlockSize>::ptr io,
				       block_address location,
				       block_type bt,
				       typename validator::ptr v,
				       bool zero)
	: io_(io),
	  location_(location),
	  data_(new buffer<BlockSize>()),
	  validator_(v),
	  bt_(bt),
	  dirty_(false)
{
	if (zero) {
		memset(data_->raw(), 0, BlockSize);
		dirty_ = true;
	} else {
		io_->read_buffer(location_, *data_);
		validator_->check(*data_, location_);
	}
}

template <uint32_t BlockSize>
block_manager<BlockSize>::block::~block()
{
	flush();
}

template <uint32_t BlockSize>
void
block_manager<BlockSize>::block::flush()
{
	if (dirty_) {
		validator_->prepare(*data_, location_);
		io_->write_buffer(location_, *data_);
	}
}

//----------------------------------------------------------------

template <uint32_t BlockSize>
block_manager<BlockSize>::read_ref::read_ref(block_manager<BlockSize> const &bm,
					     block_ptr b)
	: bm_(bm),
	  block_(b),
	  holders_(new unsigned)
{
	*holders_ = 1;
}

template <uint32_t BlockSize>
block_manager<BlockSize>::read_ref::read_ref(read_ref const &rhs)
	: bm_(rhs.bm_),
	  block_(rhs.block_),
	  holders_(rhs.holders_)
{
	(*holders_)++;
}

template <uint32_t BlockSize>
block_manager<BlockSize>::read_ref::~read_ref()
{
	if (!--(*holders_)) {
		if (block_->bt_ == BT_SUPERBLOCK) {
			bm_.flush();
			bm_.cache_.put(block_);
			bm_.flush();
		} else
			bm_.cache_.put(block_);

		delete holders_;
	}
}

template <uint32_t BlockSize>
typename block_manager<BlockSize>::read_ref const &
block_manager<BlockSize>::read_ref::operator =(read_ref const &rhs)
{
	if (this != &rhs) {
		block_ = rhs.block_;
		bm_ = rhs.bm_;
		holders_ = rhs.holders_;
		(*holders_)++;
	}
}

template <uint32_t BlockSize>
block_address
block_manager<BlockSize>::read_ref::get_location() const
{
	return block_->location_;
}

template <uint32_t BlockSize>
buffer<BlockSize> const &
block_manager<BlockSize>::read_ref::data() const
{
	return *block_->data_;
}

//--------------------------------

template <uint32_t BlockSize>
block_manager<BlockSize>::write_ref::write_ref(block_manager<BlockSize> const &bm,
					       block_ptr b)
	: read_ref(bm, b)
{
	b->dirty_ = true;
}

template <uint32_t BlockSize>
buffer<BlockSize> &
block_manager<BlockSize>::write_ref::data()
{
	return *read_ref::block_->data_;
}

//----------------------------------------------------------------

template <uint32_t BlockSize>
block_manager<BlockSize>::block_manager(std::string const &path,
					block_address nr_blocks,
					unsigned max_concurrent_blocks,
					bool writeable)
	: io_(new block_io<BlockSize>(path, nr_blocks, writeable)),
	  cache_(max(64u, max_concurrent_blocks))
{
}

template <uint32_t BlockSize>
typename block_manager<BlockSize>::read_ref
block_manager<BlockSize>::read_lock(block_address location,
				    typename block_manager<BlockSize>::validator::ptr v) const
{
	check(location);
	boost::optional<block_ptr> cached_block = cache_.get(location);

	if (cached_block) {
		(*cached_block)->check_read_lockable();
		return read_ref(*this, *cached_block);
	}

	block_ptr b(new block(io_, location, BT_NORMAL, v));
	cache_.insert(b);
	return read_ref(*this, b);
}

template <uint32_t BlockSize>
typename block_manager<BlockSize>::write_ref
block_manager<BlockSize>::write_lock(block_address location,
				     typename block_manager<BlockSize>::validator::ptr v)
{
	check(location);

	boost::optional<block_ptr> cached_block = cache_.get(location);

	if (cached_block) {
		(*cached_block)->check_write_lockable();
		return write_ref(*this, *cached_block);
	}

	block_ptr b(new block(io_, location, BT_NORMAL, v));
	cache_.insert(b);
	return write_ref(*this, b);
}

template <uint32_t BlockSize>
typename block_manager<BlockSize>::write_ref
block_manager<BlockSize>::write_lock_zero(block_address location,
					  typename block_manager<BlockSize>::validator::ptr v)
{
	check(location);

	boost::optional<block_ptr> cached_block = cache_.get(location);
	if (cached_block) {
		(*cached_block)->check_write_lockable();
		memset((*cached_block)->data_->raw(), 0, BlockSize);
		return write_ref(*this, *cached_block);
	}

	block_ptr b(new block(io_, location, BT_NORMAL, v, true));
	cache_.insert(b);
	return write_ref(*this, b);
}

template <uint32_t BlockSize>
typename block_manager<BlockSize>::write_ref
block_manager<BlockSize>::superblock(block_address location,
				     typename block_manager<BlockSize>::validator::ptr v)
{
	check(location);

	boost::optional<block_ptr> cached_block = cache_.get(location);

	if (cached_block) {
		(*cached_block)->check_write_lockable();
		(*cached_block)->bt_ = BT_SUPERBLOCK;
		(*cached_block)->validator_ = v;
		return write_ref(*this, *cached_block);
	}

	block_ptr b(new block(io_, location, BT_SUPERBLOCK, v));
	cache_.insert(b);
	return write_ref(*this, b);
}

template <uint32_t BlockSize>
typename block_manager<BlockSize>::write_ref
block_manager<BlockSize>::superblock_zero(block_address location,
					  typename block_manager<BlockSize>::validator::ptr v)
{
	check(location);

	boost::optional<block_ptr> cached_block = cache_.get(location);

	if (cached_block) {
		(*cached_block)->check_write_lockable();
		memset((*cached_block)->data_->raw(), 0, BlockSize); // FIXME: add a zero method to buffer
		(*cached_block)->validator_ = v;
		return write_ref(*this, *cached_block);
	}

	block_ptr b(new block(io_, location, BT_SUPERBLOCK,
			      mk_validator(new noop_validator), true));
	b->validator_ = v;
	cache_.insert(b);
	return write_ref(*this, b);
}

template <uint32_t BlockSize>
void
block_manager<BlockSize>::check(block_address b) const
{
	if (b >= io_->get_nr_blocks())
		throw std::runtime_error("block address out of bounds");
}

template <uint32_t BlockSize>
block_address
block_manager<BlockSize>::get_nr_blocks() const
{
	return io_->get_nr_blocks();
}

template <uint32_t BlockSize>
void
block_manager<BlockSize>::write_block(block_ptr b) const
{
	b->flush();
}

template <uint32_t BlockSize>
void
block_manager<BlockSize>::flush() const
{
	cache_.iterate_unheld(
		boost::bind(&block_manager<BlockSize>::write_block, this, _1));
}

//----------------------------------------------------------------

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
#include <fcntl.h>
#include <string.h>

#include <boost/bind.hpp>
#include <stdexcept>
#include <sstream>

// FIXME: remove these from a header!
using namespace boost;
using namespace persistent_data;
using namespace std;

//----------------------------------------------------------------

namespace {
	using namespace std;

	int const DEFAULT_MODE = 0666;
	int const OPEN_FLAGS = O_DIRECT | O_SYNC;

	// FIXME: introduce a new exception for this, or at least lift this
	// to exception.h
	void syscall_failed(char const *call) {
		char buffer[128];
		char *msg = strerror_r(errno, buffer, sizeof(buffer));

		ostringstream out;
		out << "syscall '" << call << "' failed: " << msg;
		throw runtime_error(out.str());
	}

	int open_file(string const &path, int flags) {
		int fd = ::open(path.c_str(), OPEN_FLAGS | flags, DEFAULT_MODE);
		if (fd < 0)
			syscall_failed("open");

		return fd;
	}

	bool file_exists(string const &path) {
		typename ::stat info;

		int r = ::stat(path.c_str(), &info);
		if (r) {
			if (errno == ENOENT)
				return false;

			syscall_failed("stat");
			return false; // never get here

		} else
			return S_ISREG(info.st_mode) || S_ISBLK(info.st_mode);
	}

	int create_block_file(string const &path, off_t file_size) {
		if (file_exists(path)) {
			ostringstream out;
			out << __FUNCTION__ << ": file '" << path << "' already exists";
			throw runtime_error(out.str());
		}

		int fd = open_file(path, O_CREAT | O_RDWR);

		// fallocate didn't seem to work
		int r = ::lseek(fd, file_size, SEEK_SET);
		if (r < 0)
			syscall_failed("lseek");

		return fd;
	}

	int open_block_file(string const &path, off_t min_size, bool writeable) {
		if (!file_exists(path)) {
			ostringstream out;
			out << __FUNCTION__ << ": file '" << path << "' doesn't exist";
			throw runtime_error(out.str());
		}

		return open_file(path, writeable ? O_RDWR : O_RDONLY);
	}
};

template <uint32_t BlockSize>
block_io<BlockSize>::block_io(std::string const &path, block_address nr_blocks, mode m)
	: nr_blocks_(nr_blocks),
	  mode_(m)
{
	off_t file_size = nr_blocks * BlockSize;

	switch (m) {
	case READ_ONLY:
		fd_ = open_block_file(path, file_size, false);
		break;

	case READ_WRITE:
		fd_ = open_block_file(path, file_size, true);
		break;

	case CREATE:
		fd_ = create_block_file(path, file_size);
		break;

	default:
		throw runtime_error("unsupported mode");
	}
}

template <uint32_t BlockSize>
block_io<BlockSize>::~block_io()
{
	if (::close(fd_) < 0)
		syscall_failed("close");
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
					     typename block::ptr b)
	: bm_(&bm),
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
			bm_->flush();
			bm_->cache_.put(block_);
			bm_->flush();
		} else
			bm_->cache_.put(block_);

		bm_->tracker_.unlock(block_->location_);
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

	return *this;
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
					       typename block::ptr b)
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
					typename block_io<BlockSize>::mode mode)
	: io_(new block_io<BlockSize>(path, nr_blocks, mode)),
	  cache_(max(64u, max_concurrent_blocks)),
	  tracker_(0, nr_blocks)
{
}

template <uint32_t BlockSize>
typename block_manager<BlockSize>::read_ref
block_manager<BlockSize>::read_lock(block_address location,
				    typename block_manager<BlockSize>::validator::ptr v) const
{
	tracker_.read_lock(location);
	try {
		check(location);
		boost::optional<typename block::ptr> cached_block = cache_.get(location);

		if (cached_block) {
			(*cached_block)->check_read_lockable();
			return read_ref(*this, *cached_block);
		}

		typename block::ptr b(new block(io_, location, BT_NORMAL, v));
		cache_.insert(b);
		return read_ref(*this, b);

	} catch (...) {
		tracker_.unlock(location);
		throw;
	}
}

template <uint32_t BlockSize>
typename block_manager<BlockSize>::write_ref
block_manager<BlockSize>::write_lock(block_address location,
				     typename block_manager<BlockSize>::validator::ptr v)
{
	tracker_.write_lock(location);
	try {
		check(location);

		boost::optional<typename block::ptr> cached_block = cache_.get(location);

		if (cached_block) {
			(*cached_block)->check_write_lockable();
			return write_ref(*this, *cached_block);
		}

		typename block::ptr b(new block(io_, location, BT_NORMAL, v));
		cache_.insert(b);
		return write_ref(*this, b);

	} catch (...) {
		tracker_.unlock(location);
		throw;
	}

}

template <uint32_t BlockSize>
typename block_manager<BlockSize>::write_ref
block_manager<BlockSize>::write_lock_zero(block_address location,
					  typename block_manager<BlockSize>::validator::ptr v)
{
	tracker_.write_lock(location);
	try {
		check(location);

		boost::optional<typename block::ptr> cached_block = cache_.get(location);
		if (cached_block) {
			(*cached_block)->check_write_lockable();
			memset((*cached_block)->data_->raw(), 0, BlockSize);
			return write_ref(*this, *cached_block);
		}

		typename block::ptr b(new block(io_, location, BT_NORMAL, v, true));
		cache_.insert(b);
		return write_ref(*this, b);

	} catch (...) {
		tracker_.unlock(location);
		throw;
	}
}

template <uint32_t BlockSize>
typename block_manager<BlockSize>::write_ref
block_manager<BlockSize>::superblock(block_address location,
				     typename block_manager<BlockSize>::validator::ptr v)
{
	tracker_.superblock_lock(location);
	try {
		check(location);

		boost::optional<typename block::ptr> cached_block = cache_.get(location);

		if (cached_block) {
			(*cached_block)->check_write_lockable();
			(*cached_block)->bt_ = BT_SUPERBLOCK;
			(*cached_block)->validator_ = v;
			return write_ref(*this, *cached_block);
		}

		typename block::ptr b(new block(io_, location, BT_SUPERBLOCK, v));
		cache_.insert(b);
		return write_ref(*this, b);

	} catch (...) {
		tracker_.unlock(location);
		throw;
	}
}

template <uint32_t BlockSize>
typename block_manager<BlockSize>::write_ref
block_manager<BlockSize>::superblock_zero(block_address location,
					  typename block_manager<BlockSize>::validator::ptr v)
{
	tracker_.superblock_lock(location);
	try {
		check(location);

		boost::optional<typename block::ptr> cached_block = cache_.get(location);

		if (cached_block) {
			(*cached_block)->check_write_lockable();
			memset((*cached_block)->data_->raw(), 0, BlockSize); // FIXME: add a zero method to buffer
			(*cached_block)->validator_ = v;
			return write_ref(*this, *cached_block);
		}

		typename block::ptr b(new block(io_, location, BT_SUPERBLOCK,
				      mk_validator(new noop_validator), true));
		b->validator_ = v;
		cache_.insert(b);
		return write_ref(*this, b);

	} catch (...) {
		tracker_.unlock(location);
		throw;
	}
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
block_manager<BlockSize>::write_block(typename block::ptr b) const
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

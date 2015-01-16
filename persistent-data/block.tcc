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

//----------------------------------------------------------------

// FIXME: give this namesace a name
namespace {
	using namespace std;

	int const DEFAULT_MODE = 0666;
	unsigned const SECTOR_SHIFT = 9;

	int const OPEN_FLAGS = O_DIRECT;

	// FIXME: introduce a new exception for this, or at least lift this
	// to exception.h
	void syscall_failed(char const *call) {
		char buffer[128];

#ifdef STRERROR_R_CHAR_P /* GNU-specific strerror_r */
		char *msg = strerror_r(errno, buffer, sizeof(buffer));
		if (msg != buffer)
			strncpy(buffer, msg, sizeof(buffer));
#else /* POSIX strerror_r variant */
		if (strerror_r(errno, buffer, sizeof(buffer)))
			*buffer = '\0';
#endif
		ostringstream out;
		out << "syscall '" << call << "' failed: " << buffer;
		throw runtime_error(out.str());
	}

	int open_file(string const &path, int flags) {
		int fd = ::open(path.c_str(), OPEN_FLAGS | flags, DEFAULT_MODE);
		if (fd < 0)
			syscall_failed("open");

		return fd;
	}

	bool file_exists(string const &path) {
		struct ::stat info;

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

		int r = ::ftruncate(fd, file_size);
		if (r < 0)
			syscall_failed("ftruncate");

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

namespace persistent_data {
	template <uint32_t BlockSize>
	block_manager<BlockSize>::read_ref::read_ref(block_cache::block &b)
		: b_(b)
	{
	}

	template <uint32_t BlockSize>
	block_manager<BlockSize>::read_ref::read_ref(read_ref const &rhs)
		: b_(rhs.b_)
	{
		b_.get();
	}

	template <uint32_t BlockSize>
	block_manager<BlockSize>::read_ref::~read_ref()
	{
		b_.put();
	}

	template <uint32_t BlockSize>
	typename block_manager<BlockSize>::read_ref const &
	block_manager<BlockSize>::read_ref::operator =(read_ref const &rhs)
	{
		if (this != &rhs) {
			b_ = rhs.b_;
			b_.get();
		}

		return *this;
	}

	template <uint32_t BlockSize>
	block_address
	block_manager<BlockSize>::read_ref::get_location() const
	{
		return b_.get_index();
	}

	template <uint32_t BlockSize>
	void const *
	block_manager<BlockSize>::read_ref::data() const
	{
		return b_.get_data();
	}

	//--------------------------------

	template <uint32_t BlockSize>
	block_manager<BlockSize>::write_ref::write_ref(block_cache::block &b)
		: read_ref(b),
		  ref_count_(NULL)
	{
	}

	template <uint32_t BlockSize>
	block_manager<BlockSize>::write_ref::write_ref(block_cache::block &b, unsigned &ref_count)
		: read_ref(b),
		  ref_count_(&ref_count) {
		if (*ref_count_)
			throw std::runtime_error("superblock already locked");
		(*ref_count_)++;
	}

	template <uint32_t BlockSize>
	block_manager<BlockSize>::write_ref::write_ref(write_ref const &rhs)
		: read_ref(rhs),
		  ref_count_(rhs.ref_count_) {
		if (ref_count_)
			(*ref_count_)++;
	}

	template <uint32_t BlockSize>
	block_manager<BlockSize>::write_ref::~write_ref()
	{
		if (ref_count_) {
			if (!*ref_count_)
				throw std::runtime_error("write_ref ref_count going below zero");

			(*ref_count_)--;
		}
	}

	template <uint32_t BlockSize>
	typename block_manager<BlockSize>::write_ref const &
	block_manager<BlockSize>::write_ref::operator =(write_ref const &rhs)
	{
		if (&rhs != this) {
			read_ref::operator =(rhs);
			ref_count_ = rhs.ref_count_;
			if (ref_count_)
				(*ref_count_)++;
		}
	}

	template <uint32_t BlockSize>
	void *
	block_manager<BlockSize>::write_ref::data()
	{
		return read_ref::b_.get_data();
	}

	//----------------------------------------------------------------

	template <uint32_t BlockSize>
	block_manager<BlockSize>::block_manager(std::string const &path,
						block_address nr_blocks,
						unsigned max_concurrent_blocks,
						mode m)
		: fd_(open_or_create_block_file(path, nr_blocks * BlockSize, m)),
		  bc_(fd_, BlockSize >> SECTOR_SHIFT, nr_blocks, 1024u * 1024u * 16),
		  superblock_ref_count_(0)
	{
	}

	template <uint32_t BlockSize>
	int
	block_manager<BlockSize>::open_or_create_block_file(string const &path, off_t file_size, mode m)
	{
		switch (m) {
		case READ_ONLY:
			return open_block_file(path, file_size, false);

		case READ_WRITE:
			return open_block_file(path, file_size, true);

		case CREATE:
			return create_block_file(path, file_size);

		default:
			throw std::runtime_error("unsupported mode");
		}
	}

	template <uint32_t BlockSize>
	typename block_manager<BlockSize>::read_ref
	block_manager<BlockSize>::read_lock(block_address location,
					    typename bcache::validator::ptr v) const
	{
		block_cache::block &b = bc_.get(location, 0, v);
		return read_ref(b);
	}

	template <uint32_t BlockSize>
	typename block_manager<BlockSize>::write_ref
	block_manager<BlockSize>::write_lock(block_address location,
					     typename bcache::validator::ptr v)
	{
		block_cache::block &b = bc_.get(location, block_cache::GF_DIRTY, v);
		return write_ref(b);
	}

	template <uint32_t BlockSize>
	typename block_manager<BlockSize>::write_ref
	block_manager<BlockSize>::write_lock_zero(block_address location,
						  typename bcache::validator::ptr v)
	{
		block_cache::block &b = bc_.get(location, block_cache::GF_ZERO, v);
		return write_ref(b);
	}

	template <uint32_t BlockSize>
	typename block_manager<BlockSize>::write_ref
	block_manager<BlockSize>::superblock(block_address location,
					     typename bcache::validator::ptr v)
	{
		if (bc_.get_nr_locked() > 0)
			throw std::runtime_error("attempt to lock superblock while other locks are still held");

		block_cache::block &b = bc_.get(location, block_cache::GF_DIRTY | block_cache::GF_BARRIER, v);
		return write_ref(b, superblock_ref_count_);
	}

	template <uint32_t BlockSize>
	typename block_manager<BlockSize>::write_ref
	block_manager<BlockSize>::superblock_zero(block_address location,
						  typename bcache::validator::ptr v)
	{
		if (bc_.get_nr_locked() > 0)
			throw std::runtime_error("attempt to lock superblock while other locks are still held");

		block_cache::block &b = bc_.get(location, block_cache::GF_ZERO | block_cache::GF_BARRIER, v);
		return write_ref(b, superblock_ref_count_);
	}

	template <uint32_t BlockSize>
	block_address
	block_manager<BlockSize>::get_nr_blocks() const
	{
		return bc_.get_nr_blocks();
	}

	template <uint32_t BlockSize>
	void
	block_manager<BlockSize>::prefetch(block_address b) const
	{
		bc_.prefetch(b);
	}

	template <uint32_t BlockSize>
	void
	block_manager<BlockSize>::flush() const
	{
		bc_.flush();
	}
}

//----------------------------------------------------------------

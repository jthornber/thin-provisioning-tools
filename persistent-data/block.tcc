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

	 // FIXME: these will slow it down until we start doing async io.
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

namespace persistent_data {
	template <uint32_t BlockSize>
	block_manager<BlockSize>::block::block(block_cache *bc,
					       block_address location,
					       block_type bt,
					       typename validator::ptr v,
					       bool zero)
		: validator_(v),
		bt_(bt),
		dirty_(false),
		unlocked_(false),
		buffer_(0, true) // FIXME: we don't know if it's writeable here :(
	{
		if (zero) {
			internal_ = block_cache_get(bc, location, GF_ZERO | GF_CAN_BLOCK);
			if (!internal_)
				throw std::runtime_error("Couldn't get block");
			dirty_ = true;
		} else {
			internal_ = block_cache_get(bc, location, GF_CAN_BLOCK);
			if (!internal_)
				throw std::runtime_error("Couldn't get block");

			validator_->check(buffer_, internal_->index);
		}

		buffer_.set_data(internal_->data);
	}

	template <uint32_t BlockSize>
	block_manager<BlockSize>::block::~block()
	{
		if (!unlocked_)
			unlock();
	}

	template <uint32_t BlockSize>
	void
	block_manager<BlockSize>::block::unlock()
	{
		validator_->prepare(buffer_, internal_->index);
		block_cache_put(internal_, dirty_ ? PF_DIRTY : 0);
		unlocked_ = true;
	}

	template <uint32_t BlockSize>
	typename block_manager<BlockSize>::block_type
	block_manager<BlockSize>::block::get_type() const
	{
		return bt_;
	}

	template <uint32_t BlockSize>
	uint64_t
	block_manager<BlockSize>::block::get_location() const
	{
		check_not_unlocked();
		return internal_->index;
	}

	template <uint32_t BlockSize>
	buffer<BlockSize> const &
	block_manager<BlockSize>::block::get_buffer() const
	{
		return buffer_;
	}

	template <uint32_t BlockSize>
	buffer<BlockSize> &
	block_manager<BlockSize>::block::get_buffer()
	{
		return buffer_;
	}

	template <uint32_t BlockSize>
	void
	block_manager<BlockSize>::block::mark_dirty()
	{
		check_not_unlocked();
		dirty_ = true;
	}

	template <uint32_t BlockSize>
	void
	block_manager<BlockSize>::block::change_validator(typename block_manager<BlockSize>::validator::ptr v,
							  bool check)
	{
		check_not_unlocked();
		if (v.get() != validator_.get()) {
			if (dirty_)
				// It may have already happened, by calling
				// this we ensure we're consistent.
				validator_->prepare(*internal_->data, internal_->index);

			validator_ = v;

			if (check)
				validator_->check(*internal_->data, internal_->index);
		}
	}

	template <uint32_t BlockSize>
	void
	block_manager<BlockSize>::block::check_not_unlocked() const
	{
		if (unlocked_)
			throw std::runtime_error("block prematurely unlocked");
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
			if (block_->get_type() == BT_SUPERBLOCK) {
				bm_->flush();
				block_->unlock();
				bm_->flush();
			} else
				block_->unlock();

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
		return block_->get_location();
	}

	template <uint32_t BlockSize>
	buffer<BlockSize> const &
	block_manager<BlockSize>::read_ref::data() const
	{
		return block_->get_buffer();
	}

	//--------------------------------

	template <uint32_t BlockSize>
	block_manager<BlockSize>::write_ref::write_ref(block_manager<BlockSize> const &bm,
						       typename block::ptr b)
		: read_ref(bm, b)
	{
		b->mark_dirty();
	}

	template <uint32_t BlockSize>
	buffer<BlockSize> &
	block_manager<BlockSize>::write_ref::data()
	{
		return read_ref::block_->get_buffer();
	}

	//----------------------------------------------------------------

	template <uint32_t BlockSize>
	block_manager<BlockSize>::block_manager(std::string const &path,
						block_address nr_blocks,
						unsigned max_concurrent_blocks,
						mode m)
	{
		// Open the file descriptor
		fd_ = open_block_file(path, nr_blocks * BlockSize, m == READ_WRITE);

		// Create the cache
		bc_ = block_cache_create(fd_, BlockSize << SECTOR_SHIFT, nr_blocks, 1024u * BlockSize * 1.2);
		if (!bc_)
			throw std::runtime_error("couldn't create block cache");
	}

	template <uint32_t BlockSize>
	typename block_manager<BlockSize>::read_ref
	block_manager<BlockSize>::read_lock(block_address location,
					    typename block_manager<BlockSize>::validator::ptr v) const
	{
		typename block::ptr b(new block(bc_, location, BT_NORMAL, v, false));
		return read_ref(*this, b);
	}

	template <uint32_t BlockSize>
	typename block_manager<BlockSize>::write_ref
	block_manager<BlockSize>::write_lock(block_address location,
					     typename block_manager<BlockSize>::validator::ptr v)
	{
		typename block::ptr b(new block(bc_, location, BT_NORMAL, v, false));
		return write_ref(*this, b);
	}

	template <uint32_t BlockSize>
	typename block_manager<BlockSize>::write_ref
	block_manager<BlockSize>::write_lock_zero(block_address location,
						  typename block_manager<BlockSize>::validator::ptr v)
	{
		typename block::ptr b(new block(bc_, location, BT_NORMAL, v, true));
		return write_ref(*this, b);
	}

	template <uint32_t BlockSize>
	typename block_manager<BlockSize>::write_ref
	block_manager<BlockSize>::superblock(block_address location,
					     typename block_manager<BlockSize>::validator::ptr v)
	{
		typename block::ptr b(new block(bc_, location, BT_SUPERBLOCK, v, false));
		return write_ref(*this, b);
	}

	template <uint32_t BlockSize>
	typename block_manager<BlockSize>::write_ref
	block_manager<BlockSize>::superblock_zero(block_address location,
						  typename block_manager<BlockSize>::validator::ptr v)
	{
		typename block::ptr b(new block(bc_, location, BT_SUPERBLOCK, v, true));
		return write_ref(*this, b);
	}

	template <uint32_t BlockSize>
	block_address
	block_manager<BlockSize>::get_nr_blocks() const
	{
		return block_cache_get_nr_blocks(bc_);
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
		block_cache_flush(bc_);
	}
}

//----------------------------------------------------------------

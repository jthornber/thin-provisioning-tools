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
	int const OPEN_FLAGS = O_DIRECT;

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

	inline void read_put(block_cache &bc, block_cache::block &b) {
		bc.put(b, 0);
	}

	inline void write_put(block_cache &bc, block_cache::block &b) {
		bc.put(b, block_cache::PF_DIRTY);
	}

	inline void super_put(block_cache &bc, block_cache::block &b) {
		bc.flush();
		bc.put(b, block_cache::PF_DIRTY);
		bc.flush();
	}

	template <uint32_t BlockSize>
	block_manager<BlockSize>::read_ref::read_ref(block_cache &bc,
						     block_cache::block &b,
						     put_behaviour_fn fn)
		: bc_(bc),
		  b_(b),
		  fn_(fn),
		  holders_(new unsigned)
	{
		*holders_ = 1;
	}

	template <uint32_t BlockSize>
	block_manager<BlockSize>::read_ref::read_ref(read_ref const &rhs)
		: bc_(rhs.bc_),
		  b_(rhs.b_),
		  fn_(rhs.fn_),
		  holders_(rhs.holders_)
	{
		(*holders_)++;
	}

	template <uint32_t BlockSize>
	block_manager<BlockSize>::read_ref::~read_ref()
	{
		if (!--(*holders_)) {
			fn_(bc_, b_);
			delete holders_;
		}
	}

	template <uint32_t BlockSize>
	typename block_manager<BlockSize>::read_ref const &
	block_manager<BlockSize>::read_ref::operator =(read_ref const &rhs)
	{
		if (this != &rhs) {
			bc_ = rhs.bc_;
			b_ = rhs.b_;
			fn_ = rhs.fn_;
			holders_ = rhs.holders_;
			(*holders_)++;
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
	block_manager<BlockSize>::write_ref::write_ref(block_cache &bc,
						       block_cache::block &b,
						       put_behaviour_fn fn)
		: read_ref(bc, b, fn)
	{
	}

	template <uint32_t BlockSize>
	void *
	block_manager<BlockSize>::write_ref::data()
	{
		return read_ref::b_.get_data();
	}

	//--------------------------------

	template <uint32_t BlockSize>
	block_manager<BlockSize>::super_ref::super_ref(block_cache &bc,
						       block_cache::block &b,
						       put_behaviour_fn fn)
		: write_ref(bc, b, fn) {
	}

	//----------------------------------------------------------------

	template <uint32_t BlockSize>
	block_manager<BlockSize>::block_manager(std::string const &path,
						block_address nr_blocks,
						unsigned max_concurrent_blocks,
						mode m)
		: fd_(open_block_file(path, nr_blocks * BlockSize, m == READ_WRITE)),
		  bc_(fd_, BlockSize >> SECTOR_SHIFT, nr_blocks, 1024u * 1024u * 256)
	{
	}

	template <uint32_t BlockSize>
	typename block_manager<BlockSize>::read_ref
	block_manager<BlockSize>::read_lock(block_address location,
					    typename bcache::validator::ptr v) const
	{
		block_cache::block &b = bc_.get(location, block_cache::GF_CAN_BLOCK, v);
		return read_ref(bc_, b, read_put);
	}

	template <uint32_t BlockSize>
	typename block_manager<BlockSize>::write_ref
	block_manager<BlockSize>::write_lock(block_address location,
					     typename bcache::validator::ptr v)
	{
		block_cache::block &b = bc_.get(location, block_cache::GF_CAN_BLOCK, v);
		return write_ref(bc_, b, write_put);
	}

	template <uint32_t BlockSize>
	typename block_manager<BlockSize>::write_ref
	block_manager<BlockSize>::write_lock_zero(block_address location,
						  typename bcache::validator::ptr v)
	{
		block_cache::block &b = bc_.get(location, block_cache::GF_CAN_BLOCK | block_cache::GF_ZERO, v);
		return write_ref(bc_, b, write_put);
	}

	template <uint32_t BlockSize>
	typename block_manager<BlockSize>::write_ref
	block_manager<BlockSize>::superblock(block_address location,
					     typename bcache::validator::ptr v)
	{
		block_cache::block &b = bc_.get(location, block_cache::GF_CAN_BLOCK, v);
		return super_ref(bc_, b, super_put);
	}

	template <uint32_t BlockSize>
	typename block_manager<BlockSize>::write_ref
	block_manager<BlockSize>::superblock_zero(block_address location,
						  typename bcache::validator::ptr v)
	{
		block_cache::block &b = bc_.get(location, block_cache::GF_CAN_BLOCK | block_cache::GF_ZERO, v);
		return super_ref(bc_, b, super_put);
	}

	template <uint32_t BlockSize>
	block_address
	block_manager<BlockSize>::get_nr_blocks() const
	{
		return bc_.get_nr_blocks();
	}

	template <uint32_t BlockSize>
	void
	block_manager<BlockSize>::flush() const
	{
		bc_.flush();
	}
}

//----------------------------------------------------------------

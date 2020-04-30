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

#include "base/error_string.h"
#include "base/file_utils.h"
#include "block-cache/io_engine.h"

#include <boost/bind.hpp>
#include <stdexcept>

using namespace persistent_data;

//----------------------------------------------------------------

block_manager::read_ref::read_ref(block_cache::block &b)
	: b_(b)
{
}

block_manager::read_ref::read_ref(read_ref const &rhs)
	: b_(rhs.b_)
{
	b_.get();
}

block_manager::read_ref::~read_ref()
{
	b_.put();
}

block_address
block_manager::read_ref::get_location() const
{
	return b_.get_index();
}

void const *
block_manager::read_ref::data() const
{
	return b_.get_data();
}

//--------------------------------

block_manager::write_ref::write_ref(block_cache::block &b)
	: read_ref(b),
	  ref_count_(NULL)
{
}

block_manager::write_ref::write_ref(block_cache::block &b, unsigned &ref_count)
	: read_ref(b),
	  ref_count_(&ref_count) {
	if (*ref_count_)
		throw std::runtime_error("superblock already locked");
	(*ref_count_)++;
}

block_manager::write_ref::write_ref(write_ref const &rhs)
	: read_ref(rhs),
	  ref_count_(rhs.ref_count_) {
	if (ref_count_)
		(*ref_count_)++;
}

block_manager::write_ref::~write_ref()
{
	if (ref_count_) {
		if (!*ref_count_) {
			std::cerr << "write_ref ref_count going below zero";
			::exit(1);
		}

		(*ref_count_)--;
	}
}

void *
block_manager::write_ref::data()
{
	return read_ref::b_.get_data();
}

//----------------------------------------------------------------

uint64_t
block_manager::choose_cache_size(block_address nr_blocks) const
{
	uint64_t const DEFAULT_CACHE_SIZE = 1024 * 1024 * 16;
	return std::min<uint64_t>(DEFAULT_CACHE_SIZE, MD_BLOCK_SIZE * nr_blocks);
}

block_manager::block_manager(std::string const &path,
					block_address nr_blocks,
					unsigned max_concurrent_blocks,
					mode m,
					bool excl)
	: fd_(open_or_create_block_file(path, nr_blocks * MD_BLOCK_SIZE, m, excl)),
	  bc_(fd_, MD_BLOCK_SIZE >> SECTOR_SHIFT, nr_blocks, choose_cache_size(nr_blocks)),
	  superblock_ref_count_(0)
{
}

file_utils::file_descriptor
block_manager::open_or_create_block_file(std::string const &path, off_t file_size, mode m, bool excl)
{
	switch (m) {
	case READ_ONLY:
		return file_utils::open_block_file(path, file_size, false, excl);

	case READ_WRITE:
		return file_utils::open_block_file(path, file_size, true, excl);

	case CREATE:
		return file_utils::create_block_file(path, file_size);

	default:
		throw std::runtime_error("unsupported mode");
	}
}

block_manager::read_ref
block_manager::read_lock(block_address location,
			 typename bcache::validator::ptr v) const
{
	block_cache::block &b = bc_.get(location, 0, v);
	return read_ref(b);
}

block_manager::write_ref
block_manager::write_lock(block_address location,
			  typename bcache::validator::ptr v)
{
	block_cache::block &b = bc_.get(location, block_cache::GF_DIRTY, v);
	return write_ref(b);
}

block_manager::write_ref
block_manager::write_lock_zero(block_address location,
			       typename bcache::validator::ptr v)
{
	block_cache::block &b = bc_.get(location, block_cache::GF_ZERO, v);
	return write_ref(b);
}

block_manager::write_ref
block_manager::superblock(block_address location,
			  typename bcache::validator::ptr v)
{
	if (bc_.get_nr_locked() > 0)
		throw std::runtime_error("attempt to lock superblock while other locks are still held");

	block_cache::block &b = bc_.get(location, block_cache::GF_DIRTY | block_cache::GF_BARRIER, v);
	return write_ref(b, superblock_ref_count_);
}

block_manager::write_ref
block_manager::superblock_zero(block_address location,
			       typename bcache::validator::ptr v)
{
	if (bc_.get_nr_locked() > 0)
		throw std::runtime_error("attempt to lock superblock while other locks are still held");

	block_cache::block &b = bc_.get(location, block_cache::GF_ZERO | block_cache::GF_BARRIER, v);
	return write_ref(b, superblock_ref_count_);
}

block_address
block_manager::get_nr_blocks() const
{
	return bc_.get_nr_blocks();
}

void
block_manager::prefetch(block_address b) const
{
	bc_.prefetch(b);
}

void
block_manager::flush() const
{
	bc_.flush();
}

//----------------------------------------------------------------

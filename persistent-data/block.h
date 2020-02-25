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

#ifndef BLOCK_H
#define BLOCK_H

#include "block-cache/block_cache.h"

#include <stdint.h>
#include <map>
#include <vector>

#include <boost/noncopyable.hpp>
#include <boost/optional.hpp>
#include <boost/shared_ptr.hpp>

#include <string>

//----------------------------------------------------------------

namespace persistent_data {
	using namespace bcache;

	uint32_t const MD_BLOCK_SIZE = 4096;

	template <uint32_t BlockSize = MD_BLOCK_SIZE>
	class block_manager : private boost::noncopyable {
	public:
		typedef boost::shared_ptr<block_manager> ptr;

		enum mode {
			READ_ONLY,
			READ_WRITE,
			CREATE
		};

		block_manager(std::string const &path,
			      block_address nr_blocks,
			      unsigned max_concurrent_locks,
			      mode m,
			      bool excl = true);

		class read_ref {
		public:
			static uint32_t const BLOCK_SIZE = BlockSize;

			read_ref(block_cache::block &b);

			read_ref(read_ref const &rhs);
			virtual ~read_ref();

			read_ref const &operator =(read_ref const &rhs);

			block_address get_location() const;
			void const *data() const;

		protected:
			block_cache::block &b_;
		};

		// Inherited from read_ref, since you can read a block that's write
		// locked.
		class write_ref : public read_ref {
		public:
			write_ref(block_cache::block &b);
			write_ref(block_cache::block &b, unsigned &ref_count);
			write_ref(write_ref const &rhs);
			~write_ref();

			write_ref const &operator =(write_ref const &rhs);

			using read_ref::data;
			void *data();

		private:
			unsigned *ref_count_;
		};

		// Locking methods
		read_ref
		read_lock(block_address location,
			  typename validator::ptr v =
			  typename validator::ptr(new bcache::noop_validator())) const;

		write_ref
		write_lock(block_address location,
			   typename validator::ptr v =
			   typename validator::ptr(new bcache::noop_validator()));

		write_ref
		write_lock_zero(block_address location,
				typename validator::ptr v =
				typename validator::ptr(new bcache::noop_validator()));

		// The super block is the one that should be written last.
		// Unlocking this block triggers the following events:
		//
		// i) synchronous write of all dirty blocks _except_ the
		// superblock.
		//
		// ii) synchronous write of superblock
		//
		// If any locks are held at the time of the superblock
		// being unlocked then an exception will be thrown.
		write_ref superblock(block_address b,
				     typename validator::ptr v =
				     typename validator::ptr(new bcache::noop_validator()));
		write_ref superblock_zero(block_address b,
					  typename validator::ptr v =
					  typename validator::ptr(new bcache::noop_validator()));

		block_address get_nr_blocks() const;

		void prefetch(block_address b) const;
		void flush() const;


		// This is just for unit tests, don't call in application
		// code.
		bool is_locked(block_address b) const;

	private:
		uint64_t choose_cache_size(block_address nr_blocks) const;
		file_utils::file_descriptor open_or_create_block_file(std::string const &path,
								      off_t file_size,
								      mode m, bool excl);
		void check(block_address b) const;

		file_utils::file_descriptor fd_;
		mutable block_cache bc_;
		unsigned superblock_ref_count_;
	};

	// A little utility to help build validators
	inline bcache::validator::ptr
	mk_validator(bcache::validator *v) {
		return bcache::validator::ptr(v);
	}
}

#include "block.tcc"

//----------------------------------------------------------------

#endif

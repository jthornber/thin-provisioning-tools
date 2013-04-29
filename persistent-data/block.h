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

#include "persistent-data/buffer.h"
#include "persistent-data/cache.h"
#include "persistent-data/lock_tracker.h"

#include <stdint.h>
#include <map>
#include <vector>

#include <boost/noncopyable.hpp>
#include <boost/optional.hpp>
#include <boost/shared_ptr.hpp>

#include <string>

//----------------------------------------------------------------

namespace persistent_data {

	uint32_t const MD_BLOCK_SIZE = 4096;

	typedef uint64_t block_address;

	template <uint32_t BlockSize = MD_BLOCK_SIZE>
	class block_io : private boost::noncopyable {
	public:
		typedef boost::shared_ptr<block_io> ptr;
		enum mode {
			READ_ONLY,
			READ_WRITE,
			CREATE
		};

		block_io(std::string const &path, block_address nr_blocks, mode m);
		~block_io();

		block_address get_nr_blocks() const {
			return nr_blocks_;
		}

		void read_buffer(block_address location, buffer<BlockSize> &buf) const;
		void write_buffer(block_address location, buffer<BlockSize> const &buf);

	private:
		int fd_;
		block_address nr_blocks_;
		mode mode_;
		bool writeable_;
	};

	template <uint32_t BlockSize = MD_BLOCK_SIZE>
	class block_manager : private boost::noncopyable {
	public:
		typedef boost::shared_ptr<block_manager> ptr;

		block_manager(std::string const &path,
			      block_address nr_blocks,
			      unsigned max_concurrent_locks,
			      typename block_io<BlockSize>::mode m);

		class validator {
		public:
			typedef boost::shared_ptr<validator> ptr;

			virtual ~validator() {}

			virtual void check(buffer<BlockSize> const &b, block_address location) const = 0;
			virtual void prepare(buffer<BlockSize> &b, block_address location) const = 0;
		};

		class noop_validator : public validator {
		public:
			void check(buffer<BlockSize> const &b, block_address location) const {}
			void prepare(buffer<BlockSize> &b, block_address location) const {}
		};

		enum block_type {
			BT_SUPERBLOCK,
			BT_NORMAL
		};

		struct block : private boost::noncopyable {
			typedef boost::shared_ptr<block> ptr;

			block(typename block_io<BlockSize>::ptr io,
			      block_address location,
			      block_type bt,
			      typename validator::ptr v,
			      bool zero = false);
			~block();

			void check_read_lockable() const {
				// FIXME: finish
			}

			void check_write_lockable() const {
				// FIXME: finish
			}

			void flush();

			void change_validator(typename block_manager<BlockSize>::validator::ptr v,
					      bool check = true);

			typename block_io<BlockSize>::ptr io_;
			block_address location_;
			std::auto_ptr<buffer<BlockSize> > data_;
			typename validator::ptr validator_;
			block_type bt_;
			bool dirty_;
		};

		class read_ref {
		public:
			static uint32_t const BLOCK_SIZE = BlockSize;

			read_ref(block_manager<BlockSize> const &bm,
				 typename block::ptr b);
			read_ref(read_ref const &rhs);
			virtual ~read_ref();

			read_ref const &operator =(read_ref const &rhs);

			block_address get_location() const;
			buffer<BlockSize> const &data() const;

		protected:
			block_manager<BlockSize> const *bm_;
			typename block::ptr block_;
			unsigned *holders_;
		};

		// Inherited from read_ref, since you can read a block that's write
		// locked.
		class write_ref : public read_ref {
		public:
			write_ref(block_manager<BlockSize> const &bm,
				  typename block::ptr b);

			using read_ref::data;
			buffer<BlockSize> &data();
		};

		// Locking methods
		read_ref
		read_lock(block_address location,
			  typename validator::ptr v =
			  typename validator::ptr(new noop_validator())) const;

		write_ref
		write_lock(block_address location,
			   typename validator::ptr v =
			   typename validator::ptr(new noop_validator()));

		write_ref
		write_lock_zero(block_address location,
				typename validator::ptr v =
				typename validator::ptr(new noop_validator()));

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
				     typename validator::ptr(new noop_validator()));
		write_ref superblock_zero(block_address b,
					  typename validator::ptr v =
					  typename validator::ptr(new noop_validator()));

		block_address get_nr_blocks() const;

		void flush() const;


		// This is just for unit tests, don't call in application
		// code.
		bool is_locked(block_address b) const;

	private:
		void check(block_address b) const;
		void write_block(typename block::ptr b) const;

		enum lock_type {
			READ_LOCK,
			WRITE_LOCK
		};

		struct cache_traits {
			typedef typename block::ptr value_type;
			typedef block_address key_type;

			static key_type get_key(value_type const &v) {
				return v->location_;
			}
		};

		typename block_io<BlockSize>::ptr io_;
		mutable base::cache<cache_traits> cache_;

		// FIXME: we need a dirty list as well as a cache
		mutable lock_tracker tracker_;
	};

	// A little utility to help build validators
	inline block_manager<>::validator::ptr
	mk_validator(block_manager<>::validator *v) {
		return block_manager<>::validator::ptr(v);
	}
}

#include "block.tcc"

//----------------------------------------------------------------

#endif

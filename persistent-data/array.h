// Copyright (C) 2012 Red Hat, Inc. All rights reserved.
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

#ifndef ARRAY_H
#define ARRAY_H

#include "btree.h"

//----------------------------------------------------------------

namespace persistent_data {
	struct array_block_disk {
		__le32 csum;
		__le32 max_entries;
		__le32 nr_entries;
		__le32 value_size;
		__le64 blocknr;
	} __attribute__((packed));

	template <typename ValueTraits>
	class ro_array_block {
	public:
		typedef typename ValueTraits::value_type value_type;
		typedef block_manager<>::read_ref read_ref;

		ro_array_block(read_ref rr);

		unsigned nr_entries() const;
		value_type get(unsigned index) const;

	private:
		const void *element_at(unsigned int index) const;

		read_ref rr_;
	};

	template <typename ValueTraits>
	class array_block : public ro_array_block<ValueTraits> {
	public:
		typedef typename ValueTraits::value_type value_type;
		typedef block_manager<>::write_ref write_ref;

		array_block(write_ref wr);

		// No virtual methods, so no need for a virtual destructor.
		// Not really sure inheritance is the right relationship
		// though.

		void set(unsigned index, value_type const &v);
		void inc_all_entries(typename ValueTraits::ref_counter &rc);
		void dec_all_entries(typename ValueTraits::ref_counter &rc);

		block_address address() const;

		// FIXME: why isn't this visible?
		//using ro_array_block<ValueTraits>::nr_entries();

	private:
		void *element_at(unsigned int index);

		write_ref wr_;
	};

	class BlockRefCounter {
	public:
		void inc(uint64_t const &v) {}
		void dec(uint64_t const &v) {}
	};

	template <typename ValueTraits>
	struct array_block_traits {
		typedef base::__le64 disk_type;
		typedef array_block<ValueTraits> value_type;
		typedef NoOpRefCounter<value_type> ref_counter;

		static void unpack(disk_type const &disk, value_type &value) {
			value = base::to_cpu<uint64_t>(disk);
		}

		static void pack(value_type const &value, disk_type &disk) {
			disk = base::to_disk<base::__le64>(value);
		}
	};

	template <typename ValueTraits>
	class array {
	public:
		typedef typename persistent_data::transaction_manager::ptr tm_ptr;

		typedef boost::shared_ptr<array<ValueTraits> > ptr;
		typedef typename ValueTraits::value_type value_type;

		array(tm_ptr tm,
		      typename ValueTraits::ref_counter rc,
		      unsigned nr_entries,
		      value_type const &default_value);

		array(tm_ptr tm,
		      typename ValueTraits::ref_counter rc,
		      block_address root);

		void set_root(block_address root);
		block_address get_root() const;

		void destroy();

		void grow(unsigned old_size, unsigned new_size, value_type const &v);
		void shrink(unsigned old_size, unsigned new_size);

		value_type const &get(unsigned index) const;
		void set(unsigned index, value_type const &value);


	private:
		array_block<ValueTraits> new_ablock();
		ro_array_block<ValueTraits> get_ablock(unsigned block_index) const;
		array_block<ValueTraits> shadow_ablock(unsigned block_index);

		void fill_tail_block(array_block<ValueTraits> &ab,
				     value_type v,
				     unsigned nr_entries);
		void insert_full_blocks(unsigned begin_index, unsigned end_index,
					value_type v);
		void insert_tail_block(unsigned index, unsigned nr_entries, value_type v);


		tm_ptr tm_;
		bool destroy_;
		unsigned entries_per_block_; // FIXME: initialise
		btree<1, array_block_traits<ValueTraits> > block_tree_;
		typename ValueTraits::ref_counter rc_;
	};
}

#include "array.tcc"

//----------------------------------------------------------------

#endif

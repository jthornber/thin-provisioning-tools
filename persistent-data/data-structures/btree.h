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

#ifndef BTREE_H
#define BTREE_H

#include "persistent-data/endian_utils.h"
#include "persistent-data/transaction_manager.h"

#include <boost/noncopyable.hpp>
#include <boost/optional.hpp>
#include <list>

//----------------------------------------------------------------

namespace persistent_data {

	template <typename ValueType>
	class NoOpRefCounter {
	public:
		void inc(ValueType const &v) {}
		void dec(ValueType const &v) {}
	};

	struct uint64_traits {
		typedef base::__le64 disk_type;
		typedef uint64_t value_type;
		typedef NoOpRefCounter<uint64_t> ref_counter;

		static void unpack(disk_type const &disk, value_type &value) {
			value = base::to_cpu<uint64_t>(disk);
		}

		static void pack(value_type const &value, disk_type &disk) {
			disk = base::to_disk<base::__le64>(value);
		}
	};

	namespace btree_detail {
		using namespace base;
		using namespace std;
		using namespace boost;

		uint32_t const BTREE_CSUM_XOR = 121107;

		//------------------------------------------------
		// On disk data layout for btree nodes
		enum node_flags {
			INTERNAL_NODE = 1,
			LEAF_NODE = 1 << 1
		};

		struct node_header {
			__le32 csum;
			__le32 flags;
			__le64 blocknr; /* which block this node is supposed to live in */

			__le32 nr_entries;
			__le32 max_entries;
			__le32 value_size;
			__le32 padding;
		} __attribute__((packed));

		struct disk_node {
			struct node_header header;
			__le64 keys[0];
		} __attribute__((packed));

		enum node_type {
			INTERNAL,
			LEAF
		};

		//------------------------------------------------
		// Class that acts as an interface over the raw little endian btree
		// node data.
		template <typename ValueTraits>
		class node_ref {
		public:
			explicit node_ref(block_address b, disk_node *raw);

			uint32_t get_checksum() const;

			block_address get_location() const {
				return location_;
			}

			block_address get_block_nr() const;

			node_type get_type() const;
			void set_type(node_type t);

			unsigned get_nr_entries() const;
			void set_nr_entries(unsigned n);

			unsigned get_max_entries() const;
			void set_max_entries(unsigned n);

			// FIXME: remove this, and get the constructor to do it.
			void set_max_entries(); // calculates the max for you.

			size_t get_value_size() const;
			void set_value_size(size_t);

			uint64_t key_at(unsigned i) const;
			void set_key(unsigned i, uint64_t k);

			typename ValueTraits::value_type value_at(unsigned i) const;
			void set_value(unsigned i,
				       typename ValueTraits::value_type const &v);

			// Increments the nr_entries field
			void insert_at(unsigned i,
				       uint64_t key,
				       typename ValueTraits::value_type const &v);

			// Does not increment nr_entries
			void overwrite_at(unsigned i,
					  uint64_t key,
					  typename ValueTraits::value_type const &v);

			// Copies entries from another node, appends them
			// to the back of this node.  Adjusts nr_entries.
			void copy_entries(node_ref const &rhs,
					  unsigned begin,
					  unsigned end);

			// Various searches
			int bsearch(uint64_t key, int want_hi) const;
			optional<unsigned> exact_search(uint64_t key) const;
			int lower_bound(uint64_t key) const;

			template <typename RefCounter>
			void inc_children(RefCounter &rc);

			disk_node *raw() {
				return raw_;
			}

			disk_node const *raw() const {
				return raw_;
			}

		private:
			static unsigned calc_max_entries(void);

			void *key_ptr(unsigned i) const;
			void *value_ptr(unsigned i) const;

			block_address location_;
			disk_node *raw_;
		};

		//------------------------------------------------
		//
		template <typename ValueTraits>
		node_ref<ValueTraits>
		to_node(typename block_manager<>::read_ref &b)
		{
			// FIXME: this should return a const read_ref somehow.
			return node_ref<ValueTraits>(
				b.get_location(),
				reinterpret_cast<disk_node *>(
					const_cast<unsigned char *>(b.data().raw())));
		}

		template <typename ValueTraits>
		node_ref<ValueTraits>
		to_node(typename block_manager<>::write_ref &b)
		{
			return node_ref<ValueTraits>(
				b.get_location(),
				reinterpret_cast<disk_node *>(
					const_cast<unsigned char *>(b.data().raw())));
		}

		class ro_spine : private noncopyable {
		public:
			ro_spine(transaction_manager::ptr tm)
				: tm_(tm) {
			}

			void step(block_address b);

			template <typename ValueTraits>
			node_ref<ValueTraits> get_node() {
				return to_node<ValueTraits>(spine_.back());
			}

		private:
			transaction_manager::ptr tm_;
			std::list<block_manager<>::read_ref> spine_;
		};

		class shadow_spine : private noncopyable {
		public:
			typedef transaction_manager::read_ref read_ref;
			typedef transaction_manager::write_ref write_ref;

			shadow_spine(transaction_manager::ptr tm)
				: tm_(tm) {
			}

			// true if the children of the shadow need incrementing
			bool step(block_address b);
			void step(transaction_manager::write_ref b) {
				spine_.push_back(b);
				if (spine_.size() == 1)
					root_ = spine_.front().get_location();
				else if (spine_.size() > 2)
					spine_.pop_front();
			}

			void pop() {
				spine_.pop_back();
			}

			template <typename ValueTraits>
			node_ref<ValueTraits> get_node() {
				return to_node<ValueTraits>(spine_.back());
			}

			block_address get_block() const {
				return spine_.back().get_location();
			}

			bool has_parent() const {
				return spine_.size() > 1;
			}

			node_ref<uint64_traits> get_parent() {
				if (spine_.size() < 2)
					throw std::runtime_error("no parent");

				return to_node<uint64_traits>(spine_.front());
			}

			block_address get_parent_location() const {
				return spine_.front().get_location();
			}

			block_address get_root() const {
				return root_;
			}

		private:
			transaction_manager::ptr tm_;
			std::list<block_manager<>::write_ref> spine_;
			block_address root_;
		};
	}

	template <unsigned Levels, typename ValueTraits>
	class btree {
	public:
		typedef boost::shared_ptr<btree<Levels, ValueTraits> > ptr;

		typedef uint64_t key[Levels];
		typedef typename ValueTraits::value_type value_type;
		typedef boost::optional<value_type> maybe_value;
		typedef boost::optional<std::pair<unsigned, value_type> > maybe_pair;
		typedef typename block_manager<>::read_ref read_ref;
		typedef typename block_manager<>::write_ref write_ref;
		typedef typename btree_detail::node_ref<ValueTraits> leaf_node;
		typedef typename btree_detail::node_ref<uint64_traits> internal_node;

		btree(typename persistent_data::transaction_manager::ptr tm,
		      typename ValueTraits::ref_counter rc);

		btree(typename transaction_manager::ptr tm,
		      block_address root,
		      typename ValueTraits::ref_counter rc);

		~btree();

		maybe_value lookup(key const &key) const;
		maybe_pair lookup_le(key const &key) const;
		maybe_pair lookup_ge(key const &key) const;

		void insert(key const &key, typename ValueTraits::value_type const &value);
		void remove(key const &key);

		void set_root(block_address root);
		block_address get_root() const;

		ptr clone() const;

		// free the on disk btree when the destructor is called
		void destroy();


		// Derive a class from this base class if you need to
		// inspect the individual nodes that make up a btree.
		class visitor {
		public:
			virtual ~visitor() {}
			typedef boost::shared_ptr<visitor> ptr;

			// The bool return values indicate whether the walk
			// should be continued into sub trees of the node (true == continue).
			virtual bool visit_internal(unsigned level, bool sub_root, boost::optional<uint64_t> key,
						    internal_node const &n) = 0;
			virtual bool visit_internal_leaf(unsigned level, bool sub_root, boost::optional<uint64_t> key,
							 internal_node const &n) = 0;
			virtual bool visit_leaf(unsigned level, bool sub_root, boost::optional<uint64_t> key,
						leaf_node const &n) = 0;

			virtual void visit_complete() {}
		};

		// Walks the tree in depth first order
		void visit(typename visitor::ptr visitor) const;

	private:
		template <typename ValueTraits2, typename Search>
		optional<typename ValueTraits2::value_type>
		lookup_raw(btree_detail::ro_spine &spine, block_address block, uint64_t key) const;

		template <typename ValueTraits2>
		void split_node(btree_detail::shadow_spine &spine,
				block_address parent_index,
				uint64_t key,
				bool top);

		template <typename ValueTraits2>
		void split_beneath(btree_detail::shadow_spine &spine, uint64_t key);

		template <typename ValueTraits2>
		void split_sibling(btree_detail::shadow_spine &spine,
				   block_address parent_index,
				   uint64_t key);

		template <typename ValueTraits2>
		bool
		insert_location(btree_detail::shadow_spine &spine,
				block_address block,
				uint64_t key,
				int *index);

		void walk_tree(typename visitor::ptr visitor,
			       unsigned level, bool root, boost::optional<uint64_t> key,
			       block_address b) const;

		typename persistent_data::transaction_manager::ptr tm_;
		bool destroy_;
		block_address root_;
		NoOpRefCounter<uint64_t> internal_rc_;
		typename ValueTraits::ref_counter rc_;
	};
};

#include "btree.tcc"

//----------------------------------------------------------------

#endif

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
#include "persistent-data/data-structures/ref_counter.h"

#include <boost/noncopyable.hpp>
#include <boost/optional.hpp>
#include <list>
#include <deque>

//----------------------------------------------------------------

namespace persistent_data {
	class block_ref_counter : public ref_counter<block_address> {
	public:
		block_ref_counter(space_map::ptr sm);

		virtual void set(block_address const &v, uint32_t rc);
		virtual void inc(block_address const &v);
		virtual void dec(block_address const &v);

	private:
		space_map::ptr sm_;
	};

	// FIXME: move to sep file.  I don't think it's directly used by
	// the btree code.
	struct uint64_traits {
		typedef base::le64 disk_type;
		typedef uint64_t value_type;
		typedef no_op_ref_counter<uint64_t> ref_counter;

		static void unpack(disk_type const &disk, value_type &value) {
			value = base::to_cpu<uint64_t>(disk);
		}

		static void pack(value_type const &value, disk_type &disk) {
			disk = base::to_disk<base::le64>(value);
		}
	};

	struct block_traits {
		typedef base::le64 disk_type;
		typedef block_address value_type;
		typedef block_ref_counter ref_counter;

		static void unpack(disk_type const &disk, value_type &value) {
			value = base::to_cpu<uint64_t>(disk);
		}

		static void pack(value_type const &value, disk_type &disk) {
			disk = base::to_disk<base::le64>(value);
		}
	};

	namespace btree_detail {
		using namespace base;
		using namespace std;

		uint32_t const BTREE_CSUM_XOR = 121107;

		//------------------------------------------------
		// On disk data layout for btree nodes
		enum node_flags {
			INTERNAL_NODE = 1,
			LEAF_NODE = 1 << 1
		};

		struct node_header {
			le32 csum;
			le32 flags;
			le64 blocknr; /* which block this node is supposed to live in */

			le32 nr_entries;
			le32 max_entries;
			le32 value_size;
			le32 padding;
		} __attribute__((packed));

		struct disk_node {
			struct node_header header;
			le64 keys[0];
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
			boost::optional<unsigned> exact_search(uint64_t key) const;
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

		class ro_spine : private boost::noncopyable {
		public:
			ro_spine(transaction_manager::ptr tm,
				 block_manager<>::validator::ptr v)
				: tm_(tm),
				  validator_(v) {
			}

			void step(block_address b);

			template <typename ValueTraits>
			node_ref<ValueTraits> get_node() {
				return to_node<ValueTraits>(spine_.back());
			}

		private:
			transaction_manager::ptr tm_;
			block_manager<>::validator::ptr validator_;
			std::list<block_manager<>::read_ref> spine_;
		};

		class shadow_spine : private boost::noncopyable {
		public:
			typedef transaction_manager::read_ref read_ref;
			typedef transaction_manager::write_ref write_ref;
			typedef boost::optional<block_address> maybe_block;

			shadow_spine(transaction_manager::ptr tm,
				     block_manager<>::validator::ptr v)

				: tm_(tm),
				  validator_(v) {
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

			node_ref<block_traits> get_parent() {
				if (spine_.size() < 2)
					throw std::runtime_error("no parent");

				return to_node<block_traits>(spine_.front());
			}

			block_address get_parent_location() const {
				return spine_.front().get_location();
			}

			block_address get_root() const {
				if (root_)
					return *root_;

				throw std::runtime_error("shadow spine has no root");
			}

		private:
			transaction_manager::ptr tm_;
			block_manager<>::validator::ptr validator_;
			std::list<block_manager<>::write_ref> spine_;
		        maybe_block root_;
		};

		// Used to keep a record of a nested btree's position.
		typedef std::vector<uint64_t> btree_path;

		// Used when visiting the nodes that make up a btree.
		struct node_location {
			node_location()
				: depth(0) {
			}

			void inc_depth() {
				depth++;
			}

			void push_key(uint64_t k) {
				path.push_back(k);
				depth = 0;
			}

			bool is_sub_root() const {
				return depth == 0; // && path.size();
			}

			unsigned level() const {
				return path.size();
			}

			// Keys used to access this sub tree
			btree_path path;

			// in this sub tree
			unsigned depth;

			// This is the key from the parent node to this
			// node.  If this node is a root then there will be
			// no parent, and hence no key.
			boost::optional<uint64_t> key;
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
		typedef typename btree_detail::node_ref<block_traits> internal_node;

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
			typedef boost::shared_ptr<visitor> ptr;
			typedef btree_detail::node_location node_location;

			virtual ~visitor() {}

			// The bool return values indicate whether the walk
			// should be continued into sub trees of the node (true == continue).
			virtual bool visit_internal(node_location const &l,
						    internal_node const &n) = 0;
			virtual bool visit_internal_leaf(node_location const &l,
							 internal_node const &n) = 0;
			virtual bool visit_leaf(node_location const &l,
						leaf_node const &n) = 0;

			virtual void visit_complete() {}


			enum error_outcome {
				EXCEPTION_HANDLED,
				RETHROW_EXCEPTION
			};

			virtual error_outcome error_accessing_node(node_location const &l, block_address b,
							  std::string const &what) {
				return RETHROW_EXCEPTION;
			}
		};

		// Walks the tree in depth first order
		void visit_depth_first(visitor &visitor) const;

	private:
		template <typename ValueTraits2, typename Search>
		boost::optional<typename ValueTraits2::value_type>
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

		template <typename ValueTraits2, typename RC>
		bool
		insert_location(btree_detail::shadow_spine &spine,
				block_address block,
				uint64_t key,
				int *index,
				RC &leaf_rc);

		void walk_tree(visitor &visitor,
			       btree_detail::node_location const &loc,
			       block_address b) const;

		void walk_tree_internal(visitor &visitor,
					btree_detail::node_location const &loc,
					block_address b) const;

		template <typename ValueTraits2, typename RefCounter>
		void inc_children(btree_detail::shadow_spine &spine,
				  RefCounter &leaf_rc);

		typename persistent_data::transaction_manager::ptr tm_;
		bool destroy_;
		block_address root_;
		block_ref_counter internal_rc_;
		typename ValueTraits::ref_counter rc_;
		typename block_manager<>::validator::ptr validator_;
	};
};

#include "btree.tcc"

//----------------------------------------------------------------

#endif

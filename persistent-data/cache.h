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

#ifndef CACHE_H
#define CACHE_H

#include "deleter.h"

#include <boost/intrusive/circular_list_algorithms.hpp>
#include <boost/intrusive/rbtree_algorithms.hpp>
#include <boost/optional.hpp>
#include <list>
#include <map>
#include <memory>
#include <stdexcept>

//----------------------------------------------------------------

namespace base {
	// ValueTraits needs to define value_type, key_type and a get_key()
	// static function.  Commonly you will want value_type to be a
	// shared_ptr, with any teardown specific stuff in the destructor.
	template <typename ValueTraits>
	class cache {
	public:
		typedef typename ValueTraits::value_type value_type;
		typedef typename ValueTraits::key_type key_type;

		cache(unsigned max_entries);
		~cache();

		void insert(value_type const &v);

		boost::optional<value_type> get(key_type const &k);
		void put(value_type const &k);

		template <typename T>
		void iterate_unheld(T fn) const;

	private:
		void make_space();

		struct value_entry {
			// FIXME: this means the cached object must have a
			// default constructor also, which is a shame.
			// so we can construct the headers.
			value_entry()
				: ref_count_(1) {
			}

			explicit value_entry(value_type v)
				: ref_count_(1),
				  v_(v) {
			}

			struct lru {
				lru()
					: next_(0),
					  prev_(0) {
				}

				value_entry *next_, *prev_;
			};

			struct lookup {
				lookup()
					: parent_(0),
					  left_(0),
					  right_(0),
					  color_() {
				}

				value_entry *parent_, *left_, *right_;
				int color_;
			};

			lru lru_;
			lookup lookup_;
			unsigned ref_count_;
			value_type v_;
		};

		struct value_ptr_cmp {
			bool operator() (value_entry const *lhs, value_entry const *rhs) {
				key_type k1 = ValueTraits::get_key(lhs->v_);
				key_type k2 = ValueTraits::get_key(rhs->v_);

				return k1 < k2;
			}
		};

		struct key_value_ptr_cmp {
			bool operator() (key_type const &k1, value_entry const *rhs) {
				key_type k2 = ValueTraits::get_key(rhs->v_);
				return k1 < k2;
			}

			bool operator() (value_entry const *lhs, key_type const &k2) {
				key_type k1 = ValueTraits::get_key(lhs->v_);
				return k1 < k2;
			}

		};

		struct list_node_traits {
			typedef value_entry node;
			typedef value_entry *node_ptr;
			typedef const value_entry *const_node_ptr;

			static node_ptr get_next(const_node_ptr n) {
				return n->lru_.next_;
			}

			static void set_next(node_ptr n, node_ptr next) {
				n->lru_.next_ = next;
			}

			static node_ptr get_previous(const_node_ptr n) {
				return n->lru_.prev_;
			}

			static void set_previous(node_ptr n, node_ptr prev) {
				n->lru_.prev_ = prev;
			}
		};

		struct rbtree_node_traits {
			typedef value_entry node;
			typedef value_entry *node_ptr;
			typedef const value_entry * const_node_ptr;
			typedef int color;

			static node_ptr get_parent(const_node_ptr n) {
				return n->lookup_.parent_;
			}

			static void set_parent(node_ptr n, node_ptr parent) {
				n->lookup_.parent_ = parent;
			}

			static node_ptr get_left(const_node_ptr n) {
				return n->lookup_.left_;
			}

			static void set_left(node_ptr n, node_ptr left) {
				n->lookup_.left_ = left;
			}

			static node_ptr get_right(const_node_ptr n) {
				return n->lookup_.right_;
			}

			static void set_right(node_ptr n, node_ptr right) {
				n->lookup_.right_ = right;
			}

			static int get_color(const_node_ptr n) {
				return n->lookup_.color_;
			}

			static void set_color(node_ptr n, color c) {
				n->lookup_.color_ = c;
			}

			static color red() {
				return 0;
			}

			static color black() {
				return 1;
			}
		};

		typedef boost::intrusive::circular_list_algorithms<list_node_traits> lru_algo;
		typedef boost::intrusive::rbtree_algorithms<rbtree_node_traits> lookup_algo;

		unsigned max_entries_;
		unsigned current_entries_;

		value_entry lru_header_;
		value_entry lookup_header_;
	};

	template <typename ValueTraits>
	cache<ValueTraits>::cache(unsigned max_entries)
		: max_entries_(max_entries),
		  current_entries_(0) {
		lru_algo::init_header(&lru_header_);
		lookup_algo::init_header(&lookup_header_);
	}

	template <typename ValueTraits>
	cache<ValueTraits>::~cache() {
		utils::deleter<value_entry> d;
		lookup_algo::clear_and_dispose(&lookup_header_, d);
	}

	template <typename ValueTraits>
	void
	cache<ValueTraits>::insert(value_type const &v) {
		make_space();

		std::auto_ptr<value_entry> node(new value_entry(v));
		value_ptr_cmp cmp;
		lookup_algo::insert_equal(&lookup_header_, &lookup_header_, node.get(), cmp);
		node.release();
		current_entries_++;
	}

	template <typename ValueTraits>
	boost::optional<typename ValueTraits::value_type>
	cache<ValueTraits>::get(key_type const &k) {
		key_value_ptr_cmp cmp;
		value_entry *node = lookup_algo::find(&lookup_header_, k, cmp);
		if (node == &lookup_header_)
			return boost::optional<value_type>();

		if (!node->ref_count_++)
			lru_algo::unlink(node);
		return boost::optional<value_type>(node->v_);
	}

	template <typename ValueTraits>
	void
	cache<ValueTraits>::put(value_type const &v) {
		// FIXME: the lookup will go once we use a proper hook
		key_value_ptr_cmp cmp;
		key_type k = ValueTraits::get_key(v);
		value_entry *node = lookup_algo::find(&lookup_header_, k, cmp);
		if (node == &lookup_header_)
			throw std::runtime_error("invalid put");

		if (node->ref_count_ == 0)
			throw std::runtime_error("invalid put");

		if (!--node->ref_count_)
			lru_algo::link_after(&lru_header_, node);
	}

	template <typename ValueTraits>
	void
	cache<ValueTraits>::make_space() {
		if (current_entries_ == max_entries_) {
			value_entry *node = lru_header_.lru_.prev_;
			if (node == &lru_header_)
				throw std::runtime_error("cache full");

			lru_algo::unlink(node);
			lookup_algo::unlink(node);
			delete node;
			current_entries_--;
		}
	}

	template <typename ValueTraits>
	template <typename T>
	void
	cache<ValueTraits>::iterate_unheld(T fn) const {
		value_entry *n = lru_header_.lru_.next_;
		while (n != &lru_header_) {
			fn(n->v_);
			n = n->lru_.next_;
		}
	}
}

//----------------------------------------------------------------

#endif

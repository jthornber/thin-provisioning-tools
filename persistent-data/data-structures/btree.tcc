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

#include "btree.h"

#include "persistent-data/errors.h"
#include "persistent-data/checksum.h"
#include "persistent-data/transaction_manager.h"
#include "persistent-data/validators.h"

#include <iostream>
#include <sstream>
#include <stack>

//----------------------------------------------------------------

namespace {
	using namespace base;
	using namespace persistent_data;
	using namespace btree_detail;
	using namespace std;

	struct frame {
		frame(block_address blocknr,
		      uint32_t level,
		      uint32_t nr_entries)
			: blocknr_(blocknr),
			  level_(level),
			  nr_entries_(nr_entries),
			  current_child_(0) {
		}
		block_address blocknr_;
		uint32_t level_;
		uint32_t nr_entries_;
		uint32_t current_child_;
	};

	// stack for postorder DFS traversal
	// TODO: Refactor it into a spine-like class, e.g., btree_del_spine,
	//       "Spine" sounds better for btree operations.
	struct btree_del_stack {
	public:
		btree_del_stack(transaction_manager &tm): tm_(tm) {
		}

		void push_frame(block_address blocknr,
				uint32_t level,
				uint32_t nr_entries) {
			if (tm_.get_sm()->get_count(blocknr) > 1)
				tm_.get_sm()->dec(blocknr);
			else
				spine_.push(frame(blocknr, level, nr_entries));
		}

		void pop_frame() {
			tm_.get_sm()->dec(spine_.top().blocknr_);
			spine_.pop();
		}

		frame &top_frame() {
			return spine_.top();
		}

		bool is_empty() {
			return spine_.empty();
		}

	private:
		transaction_manager &tm_;
		std::stack<frame> spine_;
	};
}

//----------------------------------------------------------------

namespace persistent_data {

	inline void
	ro_spine::step(block_address b)
	{
		spine_.push_back(tm_.read_lock(b, validator_));
		if (spine_.size() > 2)
			spine_.pop_front();
	}

	inline bool
	shadow_spine::step(block_address b)
	{
		pair<write_ref, bool> p = tm_.shadow(b, validator_);
		try {
			step(p.first);
		} catch (...) {
			tm_.get_sm()->dec(p.first.get_location());
			throw;
		}
		return p.second;
	}

//----------------------------------------------------------------

	template <typename ValueTraits>
	node_ref<ValueTraits>::node_ref(block_address location, disk_node *raw)
		: location_(location),
		  raw_(raw),
		  checked_(false)
	{
	}

	template <typename ValueTraits>
	uint32_t
	node_ref<ValueTraits>::get_checksum() const
	{
		return to_cpu<uint32_t>(raw_->header.csum);
	}

	template <typename ValueTraits>
	block_address
	node_ref<ValueTraits>::get_block_nr() const
	{
		return to_cpu<uint64_t>(raw_->header.blocknr);
	}

	template <typename ValueTraits>
	btree_detail::node_type
	node_ref<ValueTraits>::get_type() const
	{
		uint32_t flags = to_cpu<uint32_t>(raw_->header.flags);
		if (flags & INTERNAL_NODE) {
			if (flags & LEAF_NODE) {
				ostringstream out;
				out << "btree node is both internal and leaf"
				    << " (block " << location_ << ")";
				throw runtime_error(out.str());
			}
			return INTERNAL;

		} else if (flags & LEAF_NODE)
			return LEAF;
		else {
			ostringstream out;
			out << "unknown node type"
			    << " (block " << location_ << ")";
			throw runtime_error(out.str());
		}
	}

	template <typename ValueTraits>
	void
	node_ref<ValueTraits>::set_type(node_type t)
	{
		uint32_t flags = to_cpu<uint32_t>(raw_->header.flags);
		switch (t) {
		case INTERNAL:
			flags = INTERNAL_NODE;
			break;

		case LEAF:
			flags = LEAF_NODE;
			break;
		}
		raw_->header.flags = to_disk<le32>(flags);
	}

	template <typename ValueTraits>
	unsigned
	node_ref<ValueTraits>::get_nr_entries() const
	{
		return to_cpu<uint32_t>(raw_->header.nr_entries);
	}

	template <typename ValueTraits>
	void
	node_ref<ValueTraits>::set_nr_entries(unsigned n)
	{
		raw_->header.nr_entries = to_disk<le32>(n);
	}

	template <typename ValueTraits>
	unsigned
	node_ref<ValueTraits>::get_max_entries() const
	{
		return to_cpu<uint32_t>(raw_->header.max_entries);
	}

	template <typename ValueTraits>
	void
	node_ref<ValueTraits>::set_max_entries(unsigned n)
	{
		raw_->header.max_entries = to_disk<le32>(n);
	}

	template <typename ValueTraits>
	void
	node_ref<ValueTraits>::set_max_entries()
	{
		set_max_entries(calc_max_entries());
	}

	template <typename ValueTraits>
	size_t
	node_ref<ValueTraits>::get_value_size() const
	{
		return to_cpu<uint32_t>(raw_->header.value_size);
	}

	template <typename ValueTraits>
	void
	node_ref<ValueTraits>::set_value_size(size_t s)
	{
		raw_->header.value_size = to_disk<le32>(static_cast<uint32_t>(s));
	}

	template <typename ValueTraits>
	uint64_t
	node_ref<ValueTraits>::key_at(unsigned i) const
	{
		if (i >= get_nr_entries())
			throw runtime_error("key index out of bounds");
		return to_cpu<uint64_t>(raw_->keys[i]);
	}

	template <typename ValueTraits>
	void
	node_ref<ValueTraits>::set_key(unsigned i, uint64_t k)
	{
		raw_->keys[i] = to_disk<le64>(k);
	}

	template <typename ValueTraits>
	typename ValueTraits::value_type
	node_ref<ValueTraits>::value_at(unsigned i) const
	{
		if (i >= get_nr_entries())
			throw runtime_error("value index out of bounds");

		// We have to copy because of alignment issues.
		typename ValueTraits::disk_type d;
		::memcpy(&d, value_ptr(i), sizeof(d));

		typename ValueTraits::value_type v;
		ValueTraits::unpack(d, v);
		return v;
	}

	template <typename ValueTraits>
	void
	node_ref<ValueTraits>::set_value(unsigned i,
					 typename ValueTraits::value_type const &v)
	{
		typename ValueTraits::disk_type d;
		ValueTraits::pack(v, d);
		::memcpy(value_ptr(i), &d, sizeof(d));
	}

	template <typename ValueTraits>
	void
	node_ref<ValueTraits>::insert_at(unsigned i,
					 uint64_t key,
					 typename ValueTraits::value_type const &v)
	{
		unsigned n = get_nr_entries();
		if ((n + 1) > get_max_entries())
			throw runtime_error("too many entries");

		set_nr_entries(n + 1);
		::memmove(key_ptr(i + 1), key_ptr(i), sizeof(uint64_t) * (n - i));
		::memmove(value_ptr(i + 1), value_ptr(i), sizeof(typename ValueTraits::disk_type) * (n - i));
		overwrite_at(i, key, v);
	}

	template <typename ValueTraits>
	void
	node_ref<ValueTraits>::overwrite_at(unsigned i,
					    uint64_t key,
					    typename ValueTraits::value_type const &v)
	{
		set_key(i, key);
		set_value(i, v);
	}

	template <typename ValueTraits>
	void
	node_ref<ValueTraits>::delete_at(unsigned i)
	{
		unsigned nr_entries = get_nr_entries();
		if (i >= nr_entries)
			throw runtime_error("key index out of bounds");
		unsigned nr_to_copy = nr_entries - (i + 1);

		if (nr_to_copy) {
			::memmove(key_ptr(i), key_ptr(i + 1), sizeof(uint64_t) * nr_to_copy);
			::memmove(value_ptr(i), value_ptr(i + 1), sizeof(typename ValueTraits::disk_type) * nr_to_copy);
		}

		set_nr_entries(nr_entries - 1);
	}

	template <typename ValueTraits>
	void
	node_ref<ValueTraits>::copy_entries(node_ref const &rhs,
					    unsigned begin,
					    unsigned end)
	{
		unsigned count = end - begin;
		unsigned n = get_nr_entries();
		if ((n + count) > get_max_entries())
			throw runtime_error("too many entries");

		::memcpy(key_ptr(n), rhs.key_ptr(begin), sizeof(uint64_t) * count);
		::memcpy(value_ptr(n), rhs.value_ptr(begin), sizeof(typename ValueTraits::disk_type) * count);
		set_nr_entries(n + count);
	}

	template <typename ValueTraits>
	void
	node_ref<ValueTraits>::move_entries(node_ref<ValueTraits> &rhs,
					    int count)
	{
		if (!count)
			return;

		unsigned nr_left = get_nr_entries();
		unsigned nr_right = rhs.get_nr_entries();
		unsigned max_entries = get_max_entries();

		if (nr_left - count > max_entries || nr_right - count > max_entries)
			throw runtime_error("too many entries");

		if (count > 0) {
			rhs.shift_entries_right(count);
			copy_entries_to_right(rhs, count);
		} else {
			copy_entries_to_left(rhs, -count);
			rhs.shift_entries_left(-count);
		}

		set_nr_entries(nr_left - count);
		rhs.set_nr_entries(nr_right + count);
	}

	template <typename ValueTraits>
	void
	node_ref<ValueTraits>::copy_entries_to_left(node_ref const &rhs, unsigned count)
	{
		unsigned n = get_nr_entries();
		if ((n + count) > get_max_entries())
			throw runtime_error("too many entries");

		::memcpy(key_ptr(n), rhs.key_ptr(0), sizeof(uint64_t) * count);
		::memcpy(value_ptr(n), rhs.value_ptr(0), sizeof(typename ValueTraits::disk_type) * count);
	}

	template <typename ValueTraits>
	void
	node_ref<ValueTraits>::copy_entries_to_right(node_ref &rhs, unsigned count) const
	{
		unsigned n = rhs.get_nr_entries();
		if ((n + count) > get_max_entries())
			throw runtime_error("too many entries");

		unsigned nr_left = get_nr_entries();
		::memcpy(rhs.key_ptr(0), key_ptr(nr_left - count), sizeof(uint64_t) * count);
		::memcpy(rhs.value_ptr(0), value_ptr(nr_left - count), sizeof(typename ValueTraits::disk_type) * count);
	}

	template <typename ValueTraits>
	void
	node_ref<ValueTraits>::shift_entries_left(unsigned shift)
	{
		unsigned n = get_nr_entries();
		if (shift > n)
			throw runtime_error("too many entries");

		unsigned nr_shifted = n - shift;
		::memmove(key_ptr(0), key_ptr(shift), sizeof(uint64_t) * nr_shifted);
		::memmove(value_ptr(0), value_ptr(shift), sizeof(typename ValueTraits::disk_type) * nr_shifted);
	}

	template <typename ValueTraits>
	void
	node_ref<ValueTraits>::shift_entries_right(unsigned shift)
	{
		unsigned n = get_nr_entries();
		if (n + shift > get_max_entries())
			throw runtime_error("too many entries");

		::memmove(key_ptr(shift), key_ptr(0), sizeof(uint64_t) * n);
		::memmove(value_ptr(shift), value_ptr(0), sizeof(typename ValueTraits::disk_type) * n);
	}

	template <typename ValueTraits>
	unsigned
	node_ref<ValueTraits>::merge_threshold() const
	{
		return get_max_entries() / 3;
	}

	template <typename ValueTraits>
	int
	node_ref<ValueTraits>::bsearch(uint64_t key, int want_hi) const
	{
		int lo = -1, hi = get_nr_entries();

		while(hi - lo > 1) {
			int mid = lo + ((hi - lo) / 2);
			uint64_t mid_key = key_at(mid);

			if (mid_key == key)
				return mid;

			if (mid_key < key)
				lo = mid;
			else
				hi = mid;
		}

		return want_hi ? hi : lo;
	}

	template <typename ValueTraits>
	boost::optional<unsigned>
	node_ref<ValueTraits>::exact_search(uint64_t key) const
	{
		int i = bsearch(key, 0);
		if (i < 0 || static_cast<unsigned>(i) >= get_nr_entries())
			return boost::optional<unsigned>();

		if (key != key_at(i))
			return boost::optional<unsigned>();

		return boost::optional<unsigned>(i);
	}

	template <typename ValueTraits>
	int
	node_ref<ValueTraits>::lower_bound(uint64_t key) const
	{
		return bsearch(key, 0);
	}

	template <typename ValueTraits>
	unsigned
	node_ref<ValueTraits>::calc_max_entries(void)
	{
		uint32_t total;

		// key + value
		size_t elt_size = sizeof(uint64_t) + sizeof(typename ValueTraits::disk_type);
		total = (MD_BLOCK_SIZE - sizeof(struct node_header)) / elt_size;
		return (total / 3) * 3; // rounds down
	}

	template <typename ValueTraits>
	void *
	node_ref<ValueTraits>::key_ptr(unsigned i) const
	{
		check_fits_within_block();

		return raw_->keys + i;
	}

	template <typename ValueTraits>
	void *
	node_ref<ValueTraits>::value_ptr(unsigned i) const
	{
		check_fits_within_block();

		void *value_base = &raw_->keys[to_cpu<uint32_t>(raw_->header.max_entries)];
		return static_cast<unsigned char *>(value_base) +
			sizeof(typename ValueTraits::disk_type) * i;
	}

	template <typename ValueTraits>
	template <typename RefCounter>
	void
	node_ref<ValueTraits>::inc_children(RefCounter &rc)
	{
		unsigned nr_entries = get_nr_entries();
		for (unsigned i = 0; i < nr_entries; i++) {
			typename ValueTraits::value_type v;
			typename ValueTraits::disk_type d;
			::memcpy(&d, value_ptr(i), sizeof(d));
			ValueTraits::unpack(d, v);
			rc.inc(v);
		}
	}

	template <typename ValueTraits>
	template <typename RefCounter>
	void
	node_ref<ValueTraits>::dec_children(RefCounter &rc)
	{
		unsigned nr_entries = get_nr_entries();
		for (unsigned i = 0; i < nr_entries; i++) {
			typename ValueTraits::value_type v;
			typename ValueTraits::disk_type d;
			::memcpy(&d, value_ptr(i), sizeof(d));
			ValueTraits::unpack(d, v);
			rc.dec(v);
		}
	}

	template <typename ValueTraits>
	bool
	node_ref<ValueTraits>::value_sizes_match() const {
		return sizeof(typename ValueTraits::disk_type) == get_value_size();
	}

	template <typename ValueTraits>
	std::string
	node_ref<ValueTraits>::value_mismatch_string() const {
		std::ostringstream out;
		out << "value size mismatch: expected " << sizeof(typename ValueTraits::disk_type)
		    << ", but got " << get_value_size()
		    << " (block " << location_ << ")." << std::endl;

		return out.str();
	}

	template <typename ValueTraits>
	void
	node_ref<ValueTraits>::check_fits_within_block() const {
		if (checked_)
			return;

		if (!value_sizes_match())
			throw std::runtime_error(value_mismatch_string());

		unsigned max = calc_max_entries();

		if (max < get_nr_entries()) {
			std::ostringstream out;
			out << "Bad nr of elements: max per block = "
			    << max << ", actual = " << get_nr_entries()
			    << " (block " << location_ << ")" << std::endl;
			throw std::runtime_error(out.str());
		}

		checked_ = true;
	}

	//--------------------------------

	template <unsigned Levels, typename ValueTraits>
	btree<Levels, ValueTraits>::
	btree(transaction_manager &tm,
	      typename ValueTraits::ref_counter rc)
		: tm_(tm),
		  destroy_(false),
		  internal_rc_(tm.get_sm()),
		  rc_(rc),
		  validator_(create_btree_node_validator())
	{
		using namespace btree_detail;

		write_ref root = tm_.new_block(validator_);

		if (Levels > 1) {
			internal_node n = to_node<block_traits>(root);
			n.set_type(btree_detail::LEAF);
			n.set_nr_entries(0);
			n.set_max_entries();
			n.set_value_size(sizeof(typename block_traits::disk_type));

		} else {
			leaf_node n = to_node<ValueTraits>(root);
			n.set_type(btree_detail::LEAF);
			n.set_nr_entries(0);
			n.set_max_entries();
			n.set_value_size(sizeof(typename ValueTraits::disk_type));
		}

		root_ = root.get_location();
	}

	template <unsigned Levels, typename ValueTraits>
	btree<Levels, ValueTraits>::
	btree(transaction_manager &tm,
	      block_address root,
	      typename ValueTraits::ref_counter rc)
		: tm_(tm),
		  destroy_(false),
		  root_(root),
		  internal_rc_(tm.get_sm()),
		  rc_(rc),
		  validator_(create_btree_node_validator())
	{
	}

	template <unsigned Levels, typename ValueTraits>
	btree<Levels, ValueTraits>::~btree()
	{

	}

	namespace {
		template <typename ValueTraits>
		struct lower_bound_search {
			static boost::optional<unsigned> search(btree_detail::node_ref<ValueTraits> n, uint64_t key) {
				int i = n.lower_bound(key);
				return (i < 0) ? boost::optional<unsigned>() : boost::optional<unsigned>(i);
			}
		};

		template <typename ValueTraits>
		struct exact_search {
			static boost::optional<unsigned> search(btree_detail::node_ref<ValueTraits> n, uint64_t key) {
				return n.exact_search(key);
			}
		};
	}

	template <unsigned Levels, typename ValueTraits>
	typename btree<Levels, ValueTraits>::maybe_value
	btree<Levels, ValueTraits>::lookup(key const &key) const
	{
		using namespace btree_detail;

		ro_spine spine(tm_, validator_);
		block_address root = root_;

		for (unsigned level = 0; level < Levels - 1; ++level) {
			boost::optional<block_address> mroot =
				lookup_raw<block_traits, lower_bound_search<block_traits> >(spine, root, key[level]);
			if (!mroot)
				return maybe_value();

			root = *mroot;
		}

		return lookup_raw<ValueTraits, exact_search<ValueTraits> >(spine, root, key[Levels - 1]);
	}

	template <unsigned Levels, typename ValueTraits>
	typename btree<Levels, ValueTraits>::maybe_pair
	btree<Levels, ValueTraits>::lookup_le(key const &key) const
	{
		using namespace btree_detail;

		return maybe_pair();
	}

	template <unsigned Levels, typename ValueTraits>
	typename btree<Levels, ValueTraits>::maybe_pair
	btree<Levels, ValueTraits>::lookup_ge(key const &key) const
	{
		using namespace btree_detail;

		return maybe_pair();
	}

	template <unsigned Levels, typename ValueTraits>
	bool
	btree<Levels, ValueTraits>::
	insert(key const &key,
	       typename ValueTraits::value_type const &value)
	{
		using namespace btree_detail;

		block_address block = root_;
		int index = 0;		// FIXME: ???
		shadow_spine spine(tm_, validator_);

		for (unsigned level = 0; level < Levels - 1; ++level) {
			bool need_insert = insert_location<block_traits>(spine, block, key[level], &index, internal_rc_);

			internal_node n = spine.template get_node<block_traits>();
			if (need_insert) {
				btree<Levels - 1, ValueTraits> new_tree(tm_, rc_);
				n.insert_at(index, key[level], new_tree.get_root());
			}

			block = n.value_at(index);
		}

		bool need_insert = insert_location<ValueTraits>(spine, block, key[Levels - 1], &index, rc_);

		leaf_node n = spine.template get_node<ValueTraits>();
		if (need_insert)
			n.insert_at(index, key[Levels - 1], value);
		else
			// FIXME: check if we're overwriting with the same value.
			n.set_value(index, value);

		root_ = spine.get_root();

		return need_insert;
	}

	template <unsigned Levels, typename ValueTraits>
	block_address
	btree<Levels, ValueTraits>::get_root() const
	{
		return root_;
	}

	template <unsigned Levels, typename ValueTraits>
	void
	btree<Levels, ValueTraits>::set_root(block_address root)
	{
		using namespace btree_detail;
		root_ = root;
	}

	template <unsigned Levels, typename ValueTraits>
	typename btree<Levels, ValueTraits>::ptr
	btree<Levels, ValueTraits>::clone() const
	{
		tm_.get_sm()->inc(root_);
		return ptr(new btree<Levels, ValueTraits>(tm_, root_, rc_));
	}

	template <unsigned Levels, typename ValueTraits>
	void
	btree<Levels, ValueTraits>::destroy()
	{
		using namespace btree_detail;

		btree_del_stack s(tm_);

		{
			read_ref blk = tm_.read_lock(root_, validator_);
			internal_node n = to_node<block_traits>(blk);
			s.push_frame(root_, 0, n.get_nr_entries());
		}

		while (!s.is_empty()) {
			frame &f = s.top_frame();

			if (f.current_child_ >= f.nr_entries_) {
				s.pop_frame();
				continue;
			}

			// FIXME: Cache the read_ref object in the stack to avoid temporary objects?
			read_ref current = tm_.read_lock(f.blocknr_, validator_);
			internal_node n = to_node<block_traits>(current);

			if (n.get_type() == INTERNAL) {
				// TODO: test performance penalty of prefetching
				//if (!f.current_child_)
				//	for (unsigned i = 0; i < n.get_nr_entries(); i++)
				//		tm_.prefetch(n.value_at(i));

				block_address b = n.value_at(f.current_child_);
				read_ref leaf = tm_.read_lock(b, validator_);
				internal_node o = to_node<block_traits>(leaf);
				s.push_frame(b, f.level_, o.get_nr_entries());
				++f.current_child_;
			// internal leaf
			} else if (f.level_ < Levels - 1) {
				block_address b = n.value_at(f.current_child_);
				read_ref leaf = tm_.read_lock(b, validator_);
				internal_node o = to_node<block_traits>(leaf);
				s.push_frame(b, f.level_ + 1, o.get_nr_entries());
				++f.current_child_;
			} else {
				leaf_node o = to_node<ValueTraits>(current);
				o.dec_children(rc_); // FIXME: move this into pop_frame()
				s.pop_frame();
			}
		}
	}

	template <unsigned Levels, typename _>
	template <typename ValueTraits, typename Search>
	boost::optional<typename ValueTraits::value_type>
	btree<Levels, _>::
	lookup_raw(ro_spine &spine, block_address block, uint64_t key) const
	{
		using namespace boost;
		typedef typename ValueTraits::value_type leaf_type;

		for (;;) {
			spine.step(block);
			node_ref<ValueTraits> leaf = spine.template get_node<ValueTraits>();

			boost::optional<unsigned> mi;
			if (leaf.get_type() == btree_detail::LEAF) {
				mi = Search::search(leaf, key);
				if (!mi)
					return boost::optional<leaf_type>();
				return boost::optional<leaf_type>(leaf.value_at(*mi));

			}

			{
				int lb = leaf.lower_bound(key);
				if (lb < 0)
					return boost::optional<leaf_type>();

				mi = lb;
			}

			node_ref<block_traits> internal = spine.template get_node<block_traits>();
			block = internal.value_at(*mi);
		}
	}


	template <unsigned Levels, typename _>
	template <typename ValueTraits>
	void
	btree<Levels, _>::
	split_node(btree_detail::shadow_spine &spine,
		   block_address parent_index,
		   uint64_t key,
		   bool top)
	{
		node_ref<ValueTraits> n = spine.template get_node<ValueTraits>();
		if (n.get_nr_entries() == n.get_max_entries()) {
			if (top)
				split_beneath<ValueTraits>(spine, key);
			else
				split_sibling<ValueTraits>(spine, parent_index, key);
		}
	}

	template <unsigned Levels, typename _>
	template <typename ValueTraits>
	void
	btree<Levels, _>::
	split_beneath(btree_detail::shadow_spine &spine,
		      uint64_t key)
	{
		using namespace btree_detail;

		node_type type;
		unsigned nr_left, nr_right;

		write_ref left = tm_.new_block(validator_);
		node_ref<ValueTraits> l = to_node<ValueTraits>(left);
		l.set_nr_entries(0);
		l.set_max_entries();
		l.set_value_size(sizeof(typename ValueTraits::disk_type));

		write_ref right = tm_.new_block(validator_);
		node_ref<ValueTraits> r = to_node<ValueTraits>(right);
		r.set_nr_entries(0);
		r.set_max_entries();
		r.set_value_size(sizeof(typename ValueTraits::disk_type));

		{
			node_ref<ValueTraits> p = spine.template get_node<ValueTraits>();

			if (p.get_value_size() != sizeof(typename ValueTraits::disk_type))
				throw std::runtime_error("bad value_size");

			nr_left = p.get_nr_entries() / 2;
			nr_right = p.get_nr_entries() - nr_left;
			type = p.get_type();

			l.set_type(type);
			l.copy_entries(p, 0, nr_left);

			r.set_type(type);
			r.copy_entries(p, nr_left, nr_left + nr_right);
		}

		{
			// The parent may have changed value type, so we re-get it.
			internal_node p = spine.template get_node<block_traits>();
			p.set_type(btree_detail::INTERNAL);
			p.set_max_entries();
			p.set_nr_entries(2);
			p.set_value_size(sizeof(typename block_traits::disk_type));

			p.overwrite_at(0, l.key_at(0), left.get_location());
			p.overwrite_at(1, r.key_at(0), right.get_location());
		}

		if (key < r.key_at(0))
			spine.step(left);
		else
			spine.step(right);
	}

	template <unsigned Levels, typename _>
	template <typename ValueTraits>
	void
	btree<Levels, _>::
	split_sibling(btree_detail::shadow_spine &spine,
		      block_address parent_index,
		      uint64_t key)
	{
		using namespace btree_detail;

		node_ref<ValueTraits> l = spine.template get_node<ValueTraits>();
		block_address left = spine.get_block();

		write_ref right = tm_.new_block(validator_);
		node_ref<ValueTraits> r = to_node<ValueTraits>(right);

		unsigned nr_left = l.get_nr_entries() / 2;
		unsigned nr_right = l.get_nr_entries() - nr_left;

		r.set_nr_entries(0);
		r.set_max_entries();
		r.set_type(l.get_type());
		r.set_value_size(sizeof(typename ValueTraits::disk_type));
		r.copy_entries(l, nr_left, nr_left + nr_right);
		l.set_nr_entries(nr_left);

		internal_node p = spine.get_parent();
		p.overwrite_at(parent_index, l.key_at(0), left);
		p.insert_at(parent_index + 1, r.key_at(0), right.get_location());

		if (key >= r.key_at(0)) {
			spine.pop();
			spine.step(right);
		}
	}

	// Returns true if we need a new insertion, rather than overwrite.
	template <unsigned Levels, typename _>
	template <typename ValueTraits, typename RC>
	bool
	btree<Levels, _>::
	insert_location(btree_detail::shadow_spine &spine,
			block_address block,
			uint64_t key,
			int *index,
			RC &leaf_rc)
	{
		using namespace btree_detail;

		bool top = true; // this isn't the same as spine.has_parent()
		int i = *index;
		bool inc = false;

		for (;;) {
			inc = spine.step(block);
			if (inc)
				inc_children<ValueTraits>(spine, leaf_rc);

			// patch up the parent to point to the new shadow
			if (spine.has_parent()) {
				internal_node p = spine.get_parent();
				p.set_value(i, spine.get_block());
			}

			internal_node internal = spine.template get_node<block_traits>();

			// Split the node if we're full
			if (internal.get_type() == INTERNAL)
				split_node<block_traits>(spine, i, key, top);
			else
				split_node<ValueTraits>(spine, i, key, top);

			internal = spine.template get_node<block_traits>();
			i = internal.lower_bound(key);
			if (internal.get_type() == btree_detail::LEAF)
				break;

			if (i < 0) {
				internal.set_key(0, key);
				i = 0;
			}

			block = internal.value_at(i);
			top = false;
		}

		node_ref<ValueTraits> leaf = spine.template get_node<ValueTraits>();
		// FIXME: gross
		if (i < 0 || leaf.key_at(i) != key)
			i++;

		// do decrement the old value if it already exists
		// FIXME: I'm not sure about this, I don't understand the |inc| reference
		if (static_cast<unsigned>(i) < leaf.get_nr_entries() && leaf.key_at(i) == key && inc) {
			// dec old entry
		}
		*index = i;

		return ((static_cast<unsigned>(i) >= leaf.get_nr_entries()) ||
			(leaf.key_at(i) != key));
	}

	template <unsigned Levels, typename ValueTraits>
	void
	btree<Levels, ValueTraits>::visit_depth_first(visitor &v) const
	{
		node_location loc;

		walk_tree(v, loc, root_);
		v.visit_complete();
	}

	template <unsigned Levels, typename ValueTraits>
	void
	btree<Levels, ValueTraits>::walk_tree(visitor &v,
					      node_location const &loc,
					      block_address b) const
	{
		try {
			walk_tree_internal(v, loc, b);

		} catch (std::runtime_error const &e) {
			switch (v.error_accessing_node(loc, b, e.what())) {
			case visitor::EXCEPTION_HANDLED:
				break;

			case visitor::RETHROW_EXCEPTION:
				throw;
			}
		}
	}

	template <unsigned Levels, typename ValueTraits>
	void
	btree<Levels, ValueTraits>::walk_tree_internal(visitor &v,
						       node_location const &loc,
						       block_address b) const
	{
		using namespace btree_detail;

		read_ref blk = tm_.read_lock(b, validator_);
		internal_node o = to_node<block_traits>(blk);

		// FIXME: use a switch statement
		if (o.get_type() == INTERNAL) {
			if (v.visit_internal(loc, o)) {
				for (unsigned i = 0; i < o.get_nr_entries(); i++)
					tm_.prefetch(o.value_at(i));

				for (unsigned i = 0; i < o.get_nr_entries(); i++) {
					node_location loc2(loc);

					loc2.inc_depth();
					loc2.key = o.key_at(i);

					walk_tree(v, loc2, o.value_at(i));
				}
			}

		} else if (loc.path.size() < Levels - 1) {
			if (v.visit_internal_leaf(loc, o))
				for (unsigned i = 0; i < o.get_nr_entries(); i++) {
					node_location loc2(loc);

					loc2.push_key(o.key_at(i));
					loc2.key = boost::optional<uint64_t>();

					walk_tree(v, loc2, o.value_at(i));
				}

		} else {
			leaf_node ov = to_node<ValueTraits>(blk);
			v.visit_leaf(loc, ov);
		}
	}


	template <unsigned Levels, typename _>
	template <typename ValueTraits, typename RefCounter>
	void
	btree<Levels, _>::inc_children(btree_detail::shadow_spine &spine, RefCounter &leaf_rc)
	{
		node_ref<block_traits> nr = spine.template get_node<block_traits>();
		if (nr.get_type() == INTERNAL)
			nr.inc_children(internal_rc_);
		else {
			node_ref<ValueTraits> leaf = spine.template get_node<ValueTraits>();
			leaf.inc_children(leaf_rc);
		}
	}
}

//----------------------------------------------------------------

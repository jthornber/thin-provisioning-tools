#include "btree.h"
#include "transaction_manager.h"

#include <iostream>

using namespace btree_detail;
using namespace persistent_data;
using namespace std;

//----------------------------------------------------------------

template <typename ValueTraits>
node_ref<ValueTraits>::node_ref(block_address location, disk_node *raw)
	: location_(location),
	  raw_(raw)
{
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
	if (flags & INTERNAL_NODE)
		return INTERNAL;
	else if (flags & LEAF_NODE)
		return LEAF;
	else
		throw runtime_error("unknown node type");
}

template <typename ValueTraits>
void
node_ref<ValueTraits>::set_type(node_type t)
{
	uint32_t flags = to_cpu<uint32_t>(raw_->header.flags);
	switch (t) {
	case INTERNAL:
		flags |= INTERNAL_NODE;
		break;

	case LEAF:
		flags |= LEAF_NODE;
		break;
	}
	raw_->header.flags = to_disk<__le32>(flags);
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
	raw_->header.nr_entries = to_disk<__le32>(n);
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
	raw_->header.max_entries = to_disk<__le32>(n);
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
	raw_->keys[i] = to_disk<__le64>(k);
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
node_ref<ValueTraits>::copy_entries(node_ref const &rhs,
				    unsigned begin,
				    unsigned end)
{
	unsigned count = end - begin;
	unsigned n = get_nr_entries();
	if ((n + count) > get_max_entries())
		throw runtime_error("too many entries");

	set_nr_entries(n + count);
	::memcpy(key_ptr(n), rhs.key_ptr(begin), sizeof(uint64_t) * count);
	::memcpy(value_ptr(n), rhs.value_ptr(begin), sizeof(typename ValueTraits::disk_type) * count);
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
optional<unsigned>
node_ref<ValueTraits>::exact_search(uint64_t key) const
{
	int i = bsearch(key, 0);
	if (i < 0 || static_cast<unsigned>(i) >= get_nr_entries())
		return optional<unsigned>();

	return optional<unsigned>(i);
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
	return raw_->keys + i;
}

template <typename ValueTraits>
void *
node_ref<ValueTraits>::value_ptr(unsigned i) const
{
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

//----------------------------------------------------------------

template <unsigned Levels, typename ValueTraits>
btree<Levels, ValueTraits>::
btree(typename transaction_manager::ptr tm,
      typename ValueTraits::ref_counter rc)
	: tm_(tm),
	  destroy_(false),
	  rc_(rc)
{
	using namespace btree_detail;

	write_ref root = tm_->new_block();

	leaf_node n = to_node<ValueTraits>(root);
	n.set_type(btree_detail::LEAF);
	n.set_nr_entries(0);
	n.set_max_entries();

	root_ = root.get_location();
}

template <unsigned Levels, typename ValueTraits>
btree<Levels, ValueTraits>::
btree(typename transaction_manager::ptr tm,
      block_address root,
      typename ValueTraits::ref_counter rc)
	: tm_(tm),
	  destroy_(false),
	  root_(root),
	  rc_(rc)
{
}

template <unsigned Levels, typename ValueTraits>
btree<Levels, ValueTraits>::~btree()
{

}

template <unsigned Levels, typename ValueTraits>
typename btree<Levels, ValueTraits>::maybe_value
btree<Levels, ValueTraits>::lookup(key const &key) const
{
	using namespace btree_detail;

        ro_spine spine(tm_);
	block_address root = root_;

        for (unsigned level = 0; level < Levels - 1; ++level) {
		optional<block_address> mroot =
			lookup_raw<uint64_traits>(spine, root, key[level]);
		if (!mroot)
			return maybe_value();

		root = *mroot;
        }

	return lookup_raw<ValueTraits>(spine, root, key[Levels - 1]);
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
void
btree<Levels, ValueTraits>::
insert(key const &key,
       typename ValueTraits::value_type const &value)
{
	using namespace btree_detail;

	block_address block = root_;
	int index = 0;		// FIXME: ???
	shadow_spine spine(tm_);

	for (unsigned level = 0; level < Levels - 1; ++level) {
		bool need_insert = insert_location<uint64_traits>(spine, block, key[level], &index);

		internal_node n = spine.template get_node<uint64_traits>();
		if (need_insert) {
			btree<Levels - 1, ValueTraits> new_tree(tm_, rc_);
			n.insert_at(index, key[level], new_tree.get_root());
		}

		block = n.value_at(index);
	}

	bool need_insert = insert_location<ValueTraits>(spine, block, key[Levels - 1], &index);

	leaf_node n = spine.template get_node<ValueTraits>();
	if (need_insert)
		n.insert_at(index, key[Levels - 1], value);
	else
		// FIXME: check if we're overwriting with the same value.
		n.set_value(index, value);
}

template <unsigned Levels, typename ValueTraits>
void
btree<Levels, ValueTraits>::remove(key const &key)
{
	using namespace btree_detail;
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
	tm_->get_sm()->inc(root_);
	return ptr(new btree<Levels, ValueTraits>(tm_, root_, rc_));
}

#if 0
template <unsigned Levels, typename ValueTraits>
void
btree<Levels, ValueTraits>::destroy()
{
	using namespace btree_detail;

}
#endif

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

	write_ref left = tm_->new_block();
	node_ref<ValueTraits> l = to_node<ValueTraits>(left);
	l.set_nr_entries(0);
	l.set_max_entries();

	write_ref right = tm_->new_block();
	node_ref<ValueTraits> r = to_node<ValueTraits>(right);
	r.set_nr_entries(0);
	r.set_max_entries();

	{
		node_ref<ValueTraits> p = spine.template get_node<ValueTraits>();
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
		internal_node p = spine.template get_node<uint64_traits>();
		p.set_type(btree_detail::INTERNAL);
		p.set_nr_entries(2);

		// FIXME: set the value_size
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

	write_ref right = tm_->new_block();
	node_ref<ValueTraits> r = to_node<ValueTraits>(right);

	unsigned nr_left = l.get_nr_entries() / 2;
	unsigned nr_right = l.get_nr_entries() - nr_left;

	r.set_nr_entries(0);
	r.set_type(l.get_type());
	r.set_max_entries(l.get_max_entries());
	r.copy_entries(l, nr_left, nr_left + nr_right);
	l.set_nr_entries(nr_left);

	internal_node p = spine.get_parent();
	p.overwrite_at(parent_index, l.key_at(0), left);
	p.insert_at(parent_index + 1, r.key_at(0), right.get_location());

	spine.pop();
	if (key < r.key_at(0))
		spine.step(left);
	else
		spine.step(right);
}

// Returns true if we need a new insertion, rather than overwrite.
template <unsigned Levels, typename _>
template <typename ValueTraits>
bool
btree<Levels, _>::
insert_location(btree_detail::shadow_spine &spine,
		block_address block,
		uint64_t key,
		int *index)
{
	using namespace btree_detail;

	bool top = true; // this isn't the same as spine.has_parent()
	int i = *index;
	bool inc = false;

	for (;;) {
		inc = spine.step(block);
#if 0
		if (inc)
			inc_children<ValueTraits>();
#endif

		// patch up the parent to point to the new shadow
		if (spine.has_parent()) {
			internal_node p = spine.get_parent();
			p.set_value(i, spine.get_block());
		}

		internal_node internal = spine.template get_node<uint64_traits>();

		// Split the node if we're full
		if (internal.get_type() == INTERNAL)
			split_node<uint64_traits>(spine, i, key, top);
		else
			split_node<ValueTraits>(spine, i, key, top);

		internal = spine.template get_node<uint64_traits>();
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
btree<Levels, ValueTraits>::visit(typename visitor::ptr visitor) const
{
	walk_tree(visitor, 0, boost::optional<uint64_t>(), root_);
}

template <unsigned Levels, typename ValueTraits>
void
btree<Levels, ValueTraits>::
walk_tree(typename visitor::ptr visitor,
	  unsigned level, boost::optional<uint64_t> key,
	  block_address b) const
{
	using namespace btree_detail;

	read_ref blk = tm_->read_lock(b);
	internal_node o = to_node<uint64_traits>(blk);
	if (o.get_type() == INTERNAL) {
		if (visitor->visit_internal(level, key, o))
			for (unsigned i = 0; i < o.get_nr_entries(); i++)
				walk_tree(visitor, level, o.key_at(i), o.value_at(i));

	} else if (level < Levels - 1) {
		if (visitor->visit_internal_leaf(level, key, o))
			for (unsigned i = 0; i < o.get_nr_entries(); i++)
				walk_tree(visitor, level + 1, boost::optional<uint64_t>(), o.value_at(i));

	} else {
		leaf_node ov = to_node<ValueTraits>(blk);
		visitor->visit_leaf(level, key, ov);
	}
}

//----------------------------------------------------------------

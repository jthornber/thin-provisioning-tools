#include "btree.h"

#include <boost/noncopyable.hpp>
#include <boost/optional.hpp>

using namespace boost;
using namespace persistent_data;

//----------------------------------------------------------------

namespace {
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
	} __attribute__((packed));

	struct node {
		struct node_header header;
		__le64 keys[0];
	} __attribute__((packed));


	//------------------------------------------------
	// Class that acts as an interface over the raw little endian btree
	// node data.
	class node {
	public:
		enum type {
			INTERNAL,
			LEAF
		};

		type get_type() const;
		void set_type(type t);

		unsigned get_nr_entries() const;
		void set_nr_entries(unsigned n);

		unsigned get_max_entries() const;
		void set_max_entries(unsigned n);

		uint64_t key_at(unsigned i) const;

		template <typename ValueTraits>
		typename ValueTraits::value_type value_at(unsigned i) const;

	private:
		struct node *raw_;
	};

	//------------------------------------------------
	// Various searches
	int bsearch(node const &n, uint64_t key, int want_hi)
	{
		int lo = -1, hi = n.get_nr_entries();

		while(hi - lo > 1) {
			int mid = lo + ((hi - lo) / 2);
			uint64_t mid_key = n.key_at(mid);

			if (mid_key == key)
				return mid;

			if (mid_key < key)
				lo = mid;
			else
				hi = mid;
		}

		return want_hi ? hi : lo;
	}

	optional<unsigned> exact_search(node const &n, uint64_t key) {
		int i = bsearch(n, key, 0);
		if (i < 0 || static_cast<unsigned>(i) >= n.get_nr_entries())
			return optional<unsigned>();

		return optional<unsigned>(i);
	}

	//------------------------------------------------
	//
	template <uint32_t BlockSize>
	node &to_node(typename block_manager<BlockSize>::write_ref b);

	unsigned
	calc_max_entries(uint32_t bs);

	// Spines
	template <uint32_t BlockSize>
	class ro_spine : private noncopyable {
	public:
		void step(block_address b);
		node get_node() const;

	private:

	};

	class internal_traits {
	public:
		typedef uint64_t value_type;
	};

	template <typename ValueTraits, uint32_t BlockSize, typename Search>
	optional<typename ValueTraits::value_type>
	lookup_raw(ro_spine<BlockSize> &spine, block_address block, uint64_t key) {

		using namespace boost;
		typedef typename ValueTraits::value_type leaf_type;
		typedef typename internal_traits::value_type internal_type;

		Search find;

		for (;;) {
			spine.step(block);
			node &n = spine.node();

			auto mi = find(n, key);
			if (!mi)
				return optional<leaf_type>();

			if (n.get_type() == node::LEAF)
				return optional<leaf_type>(n.value_at(*mi));

			block = n.value_at<internal_type>(*mi);
		}
	}
}


template <unsigned Levels, typename ValueTraits, uint32_t BlockSize>
btree<Levels, ValueTraits, BlockSize>::btree(shared_ptr<transaction_manager<BlockSize> > tm)
	: tm_(tm),
	  destroy_(false)
{
	write_ref root = tm_.new_block();

	node &n = to_node(root);
	n.set_type(node::LEAF);
	n.set_nr_entries(0);
	n.set_max_entries(calc_max_entries(BlockSize));

	root_ = root.location();
}

template <unsigned Levels, typename ValueTraits, uint32_t BlockSize>
btree<Levels, ValueTraits, BlockSize>::btree(shared_ptr<transaction_manager<BlockSize> > tm,
					     block_address root)
	: tm_(tm),
	  destroy_(false),
	  root_(root)
{
}

template <unsigned Levels, typename ValueTraits, uint32_t BlockSize>
btree<Levels, ValueTraits, BlockSize>::~btree()
{

}

template <unsigned Levels, typename ValueTraits, uint32_t BlockSize>
typename btree<Levels, ValueTraits, BlockSize>::maybe_value
btree<Levels, ValueTraits, BlockSize>::lookup(key const &key) const
{
        ro_spine<BlockSize> spine;
	block_address root = root_;

        for (unsigned level = 0; level < Levels - 1; ++level) {
		auto mroot = lookup_raw<internal_traits, BlockSize, exact_search>(spine, root, key[level]);
		if (!mroot)
			return maybe_value();

		root = *mroot;
        }

	return lookup_raw<ValueTraits, BlockSize, exact_search>(spine, root, key[Levels - 1]);
}

template <unsigned Levels, typename ValueTraits, uint32_t BlockSize>
typename btree<Levels, ValueTraits, BlockSize>::maybe_pair
btree<Levels, ValueTraits, BlockSize>::lookup_le(key const &key) const
{
	return maybe_pair();
}

template <unsigned Levels, typename ValueTraits, uint32_t BlockSize>
typename btree<Levels, ValueTraits, BlockSize>::maybe_pair
btree<Levels, ValueTraits, BlockSize>::lookup_ge(key const &key) const
{
	return maybe_pair();
}

#if 0
template <unsigned Levels, typename ValueTraits, uint32_t BlockSize>
void
btree<Levels, ValueTraits, BlockSize>::insert(key const &key, typename ValueTraits::value_type const &value)
{

}

template <unsigned Levels, typename ValueTraits, uint32_t BlockSize>
void
btree<Levels, ValueTraits, BlockSize>::remove(key const &key)
{

}

template <unsigned Levels, typename ValueTraits, uint32_t BlockSize>
block_address
btree<Levels, ValueTraits, BlockSize>::get_root() const
{

}

template <unsigned Levels, typename ValueTraits, uint32_t BlockSize>
void
btree<Levels, ValueTraits, BlockSize>::set_root(block_address root)
{

}

template <unsigned Levels, typename ValueTraits, uint32_t BlockSize>
block_address
btree<Levels, ValueTraits, BlockSize>::get_root() const
{

}

template <unsigned Levels, typename ValueTraits, uint32_t BlockSize>
ptr
btree<Levels, ValueTraits, BlockSize>::clone() const
{

}

template <unsigned Levels, typename ValueTraits, uint32_t BlockSize>
void
btree<Levels, ValueTraits, BlockSize>::destroy()
{

}
#endif
//----------------------------------------------------------------

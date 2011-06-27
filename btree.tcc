#include "btree.h"

#include "endian.h"
#include "transaction_manager.h"

#include <list>
#include <boost/noncopyable.hpp>
#include <boost/optional.hpp>

// FIXME: can't have using clauses in a header
using namespace base;
using namespace boost;
using namespace persistent_data;

//----------------------------------------------------------------

namespace btree_detail {
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

	struct disk_node {
		struct node_header header;
		__le64 keys[0];
	} __attribute__((packed));


	//------------------------------------------------
	// Class that acts as an interface over the raw little endian btree
	// node data.
	class node_ref {
	public:
		enum type {
			INTERNAL,
			LEAF
		};

		node_ref(disk_node *raw)
			: raw_(raw) {
		}

		type get_type() const {
			uint32_t flags = to_cpu<uint32_t>(raw_->header.flags);
			if (flags & INTERNAL_NODE)
				return INTERNAL;
			else if (flags & LEAF_NODE)
				return LEAF;
			else
				throw runtime_error("unknow node type");
		}

		void set_type(type t){
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

		unsigned get_nr_entries() const {
			return to_cpu<uint32_t>(raw_->header.nr_entries);
		}

		void set_nr_entries(unsigned n) {
			raw_->header.nr_entries = to_disk<__le32>(n);
		}

		unsigned get_max_entries() const {
			return to_cpu<uint32_t>(raw_->header.max_entries);
		}

		void set_max_entries(unsigned n) {
			raw_->header.max_entries = to_disk<__le32>(n);
		}

		uint64_t key_at(unsigned i) const {
			return to_cpu<uint64_t>(raw_->keys[i]);
		}

		template <typename ValueTraits>
		typename ValueTraits::value_type value_at(unsigned i) const {
			void *value_base = &raw_->keys[to_cpu<uint32_t>(raw_->header.max_entries)];
			void *value_ptr = static_cast<unsigned char *>(value_base) +
				sizeof(typename ValueTraits::disk_type) * i;
			return ValueTraits::construct(value_ptr);
		}

	private:
		disk_node *raw_;
	};

	//------------------------------------------------
	// Various searches
	int bsearch(node_ref const &n, uint64_t key, int want_hi)
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

	optional<unsigned> exact_search(node_ref const &n, uint64_t key) {
		int i = bsearch(n, key, 0);
		if (i < 0 || static_cast<unsigned>(i) >= n.get_nr_entries())
			return optional<unsigned>();

		return optional<unsigned>(i);
	}

	//------------------------------------------------
	//
	template <uint32_t BlockSize>
	node_ref to_node(typename block_manager<BlockSize>::read_ref &b)
	{
		// FIXME: this should return a const read_ref somehow.
		return node_ref(
			reinterpret_cast<disk_node *>(const_cast<unsigned char *>(b.data())));
	}

	template <uint32_t BlockSize>
	node_ref to_node(typename block_manager<BlockSize>::write_ref &b)
	{
		return node_ref(
			reinterpret_cast<disk_node *>(const_cast<unsigned char *>(b.data())));
	}

	unsigned
	calc_max_entries(uint32_t bs);

	// Spines
	template <uint32_t BlockSize>
	class ro_spine : private noncopyable {
	public:
		ro_spine(typename transaction_manager<BlockSize>::ptr tm)
			: tm_(tm) {
		}

		void step(block_address b) {
			spine_.push_back(tm_->read_lock(b));
			if (spine_.size() > 2)
				spine_.pop_front();
		}

		node_ref get_node() {
			return to_node<BlockSize>(spine_.back());
		}

	private:
		typename transaction_manager<BlockSize>::ptr tm_;
		std::list<typename block_manager<BlockSize>::read_ref> spine_;
	};

	template <uint32_t BlockSize>
	class shadow_spine : private noncopyable {
	public:
		shadow_spine(typename transaction_manager<BlockSize>::ptr tm)
			: tm_(tm) {
		}

		void step(block_address b) {
			spine_.push_back(tm_->shadow(b));
			if (spine_.size() == 1)
				root_ = spine_.front().get_location();
			else if (spine_.size() > 2)
				spine_.pop_front();
		}

		node_ref get_node() {
			return to_node<BlockSize>(spine_.back());
		}

		node_ref get_parent() {
			if (spine_.size() < 2)
				throw std::runtime_error("no parent");

			return to_node<BlockSize>(spine_.front());
		}

		node_ref get_root() {
			return root_;
		}

	private:
		typename transaction_manager<BlockSize>::ptr tm_;
		std::list<typename block_manager<BlockSize>::write_ref> spine_;
		block_address root_;
	};

	template <typename ValueTraits, uint32_t BlockSize> //, typename Search>
	optional<typename ValueTraits::value_type>
	lookup_raw(ro_spine<BlockSize> &spine, block_address block, uint64_t key) {

		using namespace boost;
		typedef typename ValueTraits::value_type leaf_type;

		for (;;) {
			spine.step(block);
			node_ref const &n = spine.get_node();

			auto mi = exact_search(n, key);
			if (!mi)
				return optional<leaf_type>();

			if (n.get_type() == node_ref::LEAF)
				return optional<leaf_type>(n.value_at<ValueTraits>(*mi));

			block = n.value_at<uint64_traits>(*mi);
		}
	}
}

template <unsigned Levels, typename ValueTraits, uint32_t BlockSize>
btree<Levels, ValueTraits, BlockSize>::btree(typename transaction_manager<BlockSize>::ptr tm)
	: tm_(tm),
	  destroy_(false)
{
	using namespace btree_detail;

	write_ref root = tm_->new_block();

	node_ref n = to_node<BlockSize>(root);
	n.set_type(node_ref::LEAF);
	n.set_nr_entries(0);
	n.set_max_entries(calc_max_entries(BlockSize));

	root_ = root.get_location();
}

template <unsigned Levels, typename ValueTraits, uint32_t BlockSize>
btree<Levels, ValueTraits, BlockSize>::btree(typename transaction_manager<BlockSize>::ptr tm,
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
	using namespace btree_detail;

        ro_spine<BlockSize> spine(tm_);
	block_address root = root_;

        for (unsigned level = 0; level < Levels - 1; ++level) {
		optional<block_address> mroot =
			lookup_raw<uint64_traits, BlockSize>(spine, root, key[level]);
		if (!mroot)
			return maybe_value();

		root = *mroot;
        }

	return lookup_raw<ValueTraits, BlockSize>(spine, root, key[Levels - 1]);
}

template <unsigned Levels, typename ValueTraits, uint32_t BlockSize>
typename btree<Levels, ValueTraits, BlockSize>::maybe_pair
btree<Levels, ValueTraits, BlockSize>::lookup_le(key const &key) const
{
	using namespace btree_detail;

	return maybe_pair();
}

template <unsigned Levels, typename ValueTraits, uint32_t BlockSize>
typename btree<Levels, ValueTraits, BlockSize>::maybe_pair
btree<Levels, ValueTraits, BlockSize>::lookup_ge(key const &key) const
{
	using namespace btree_detail;

	return maybe_pair();
}

#if 0
template <unsigned Levels, typename ValueTraits, uint32_t BlockSize>
void
btree<Levels, ValueTraits, BlockSize>::insert(key const &key, typename ValueTraits::value_type const &value)
{
	using namespace btree_detail;

}

template <unsigned Levels, typename ValueTraits, uint32_t BlockSize>
void
btree<Levels, ValueTraits, BlockSize>::remove(key const &key)
{
	using namespace btree_detail;

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
	using namespace btree_detail;

}

template <unsigned Levels, typename ValueTraits, uint32_t BlockSize>
block_address
btree<Levels, ValueTraits, BlockSize>::get_root() const
{
	using namespace btree_detail;

}

template <unsigned Levels, typename ValueTraits, uint32_t BlockSize>
ptr
btree<Levels, ValueTraits, BlockSize>::clone() const
{
	using namespace btree_detail;

}

template <unsigned Levels, typename ValueTraits, uint32_t BlockSize>
void
btree<Levels, ValueTraits, BlockSize>::destroy()
{
	using namespace btree_detail;

}
#endif
//----------------------------------------------------------------

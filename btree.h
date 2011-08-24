#ifndef BTREE_H
#define BTREE_H

#include "endian_utils.h"
#include "transaction_manager.h"

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
		template <typename ValueTraits, uint32_t BlockSize>
		class node_ref {
		public:
			explicit node_ref(block_address b, disk_node *raw);

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

			// FIXME: remove
			void *raw() {
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
		template <typename ValueTraits, uint32_t BlockSize>
		node_ref<ValueTraits, BlockSize>
		to_node(typename block_manager<BlockSize>::read_ref &b)
		{
			// FIXME: this should return a const read_ref somehow.
			return node_ref<ValueTraits, BlockSize>(
				b.get_location(),
				reinterpret_cast<disk_node *>(
					const_cast<unsigned char *>(b.data())));
		}

		template <typename ValueTraits, uint32_t BlockSize>
		node_ref<ValueTraits, BlockSize>
		to_node(typename block_manager<BlockSize>::write_ref &b)
		{
			return node_ref<ValueTraits, BlockSize>(
				b.get_location(),
				reinterpret_cast<disk_node *>(
					const_cast<unsigned char *>(b.data())));
		}

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

			template <typename ValueTraits>
			node_ref<ValueTraits, BlockSize> get_node() {
				return to_node<ValueTraits, BlockSize>(spine_.back());
			}

		private:
			typename transaction_manager<BlockSize>::ptr tm_;
			std::list<typename block_manager<BlockSize>::read_ref> spine_;
		};

		template <uint32_t BlockSize>
		class shadow_spine : private noncopyable {
		public:
			typedef typename transaction_manager<BlockSize>::read_ref read_ref;
			typedef typename transaction_manager<BlockSize>::write_ref write_ref;

			shadow_spine(typename transaction_manager<BlockSize>::ptr tm)
				: tm_(tm) {
			}

			// true if the children of the shadow need incrementing
			bool step(block_address b) {
				pair<write_ref, bool> p = tm_->shadow(b);
				try {
					step(p.first);
				} catch (...) {
					tm_->get_sm()->dec(p.first.get_location());
					throw;
				}
				return p.second;
			}

			void step(typename transaction_manager<BlockSize>::write_ref b) {
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
			node_ref<ValueTraits, BlockSize> get_node() {
				return to_node<ValueTraits, BlockSize>(spine_.back());
			}

			block_address get_block() const {
				return spine_.back().get_location();
			}

			bool has_parent() const {
				return spine_.size() > 1;
			}

			node_ref<uint64_traits, BlockSize> get_parent() {
				if (spine_.size() < 2)
					throw std::runtime_error("no parent");

				return to_node<uint64_traits, BlockSize>(spine_.front());
			}

			block_address get_parent_location() const {
				return spine_.front().get_location();
			}

			block_address get_root() const {
				return root_;
			}

		private:
			typename transaction_manager<BlockSize>::ptr tm_;
			std::list<typename block_manager<BlockSize>::write_ref> spine_;
			block_address root_;
		};

		// FIXME: make a member of btree
		template <typename ValueTraits, uint32_t BlockSize>
		optional<typename ValueTraits::value_type>
		lookup_raw(ro_spine<BlockSize> &spine, block_address block, uint64_t key) {

			using namespace boost;
			typedef typename ValueTraits::value_type leaf_type;

			for (;;) {
				spine.step(block);
				node_ref<ValueTraits, BlockSize> leaf = spine.template get_node<ValueTraits>();

				optional<unsigned> mi = leaf.exact_search(key);
				if (!mi)
					return optional<leaf_type>();

				if (leaf.get_type() == btree_detail::LEAF)
					return optional<leaf_type>(leaf.value_at(*mi));

				node_ref<uint64_traits, BlockSize> internal = spine.template get_node<uint64_traits>();
				block = internal.value_at(*mi);
			}
		}
	}

	template <unsigned Levels, typename ValueTraits, uint32_t BlockSize>
	class btree {
	public:
		typedef boost::shared_ptr<btree<Levels, ValueTraits, BlockSize> > ptr;

		typedef uint64_t key[Levels];
		typedef typename ValueTraits::value_type value_type;
		typedef boost::optional<value_type> maybe_value;
		typedef boost::optional<std::pair<unsigned, value_type> > maybe_pair;
		typedef typename block_manager<BlockSize>::read_ref read_ref;
		typedef typename block_manager<BlockSize>::write_ref write_ref;
		typedef typename btree_detail::node_ref<ValueTraits, BlockSize> leaf_node;
		typedef typename btree_detail::node_ref<uint64_traits, BlockSize> internal_node;

		btree(typename persistent_data::transaction_manager<BlockSize>::ptr tm,
		      typename ValueTraits::ref_counter rc);

		btree(typename transaction_manager<BlockSize>::ptr tm,
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

			virtual void visit_internal(unsigned level, bool is_root, internal_node const &n) = 0;
			virtual void visit_internal_leaf(unsigned level, bool is_root, internal_node const &n) = 0;
			virtual void visit_leaf(unsigned level, bool is_root, leaf_node const &n) = 0;
		};

		// Walks the tree in depth first order
		void visit(typename visitor::ptr visitor);

	private:
		template <typename ValueTraits2>
		void split_node(btree_detail::shadow_spine<BlockSize> &spine,
				block_address parent_index,
				uint64_t key,
				bool top);

		template <typename ValueTraits2>
		void split_beneath(btree_detail::shadow_spine<BlockSize> &spine, uint64_t key);

		template <typename ValueTraits2>
		void split_sibling(btree_detail::shadow_spine<BlockSize> &spine,
				   block_address parent_index,
				   uint64_t key);

		template <typename ValueTraits2>
		bool
		insert_location(btree_detail::shadow_spine<BlockSize> &spine,
				block_address block,
				uint64_t key,
				int *index);

		void walk_tree(typename visitor::ptr visitor,
			       unsigned level, bool is_root,
			       block_address b);

		typename persistent_data::transaction_manager<BlockSize>::ptr tm_;
		bool destroy_;
		block_address root_;
		NoOpRefCounter<uint64_t> internal_rc_;
		typename ValueTraits::ref_counter rc_;
	};
};

#include "btree.tcc"

//----------------------------------------------------------------

#endif

#ifndef BTREE_H
#define BTREE_H

#include "transaction_manager.h"

//----------------------------------------------------------------

namespace persistent_data {
	template <unsigned Levels, typename ValueTraits, uint32_t BlockSize>
	class btree {
	public:
		typedef uint64_t key[Levels];
		typedef typename ValueTraits::value_type value_type;
		typedef boost::optional<value_type> maybe_value;
		typedef boost::optional<std::pair<unsigned, value_type> > maybe_pair;
		typedef boost::shared_ptr<btree<Levels, ValueTraits, BlockSize> > ptr;
		typedef typename block_manager<BlockSize>::read_ref read_ref;
		typedef typename block_manager<BlockSize>::write_ref write_ref;

		btree(boost::shared_ptr<transaction_manager<BlockSize> > tm);
		btree(boost::shared_ptr<transaction_manager<BlockSize> > tm,
		      block_address root);
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

	private:
		typename persistent_data::transaction_manager<BlockSize>::ptr tm_;
		bool destroy_;
		block_address root_;
	};
};

//----------------------------------------------------------------

#endif

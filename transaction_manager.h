#ifndef TRANSACTION_MANAGER_H
#define TRANSACTION_MANAGER_H

#include "block.h"
#include "space_map.h"

#include <set>
#include <boost/shared_ptr.hpp>

//----------------------------------------------------------------

namespace persistent_data {
	template <uint32_t MetadataBlockSize>
	class transaction_manager : public boost::noncopyable {
	public:
		typedef boost::shared_ptr<transaction_manager<MetadataBlockSize> > ptr;

		transaction_manager(typename block_manager<MetadataBlockSize>::ptr bm,
				    space_map::ptr sm);
		~transaction_manager();

		typedef typename block_manager<MetadataBlockSize>::read_ref read_ref;
		typedef typename block_manager<MetadataBlockSize>::write_ref write_ref;
		typedef typename block_manager<MetadataBlockSize>::block_validator block_validator;

		void reserve_block(block_address location);
		void begin();
		void pre_commit();
		void commit(write_ref superblock);

		block_address alloc_block();
		write_ref new_block();
		write_ref new_block(block_validator const &v);

		write_ref shadow(block_address orig, bool &inc_children);
		write_ref shadow(block_address orig, block_validator const &v, bool &inc_children);

		read_ref read_lock(block_address b);
		read_ref read_lock(block_address b, block_validator const &v);

		void inc(block_address b);
		void dec(block_address b);
		uint32_t ref_count(block_address b) const;

	private:
		void add_shadow(block_address b);
	        bool is_shadow(block_address b);
		void wipe_shadow_table();

		typename block_manager<MetadataBlockSize>::ptr bm_;
		space_map::ptr sm_;

		std::set<block_address> shadows_;
	};
}

//----------------------------------------------------------------

#endif

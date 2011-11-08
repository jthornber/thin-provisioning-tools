#ifndef TRANSACTION_MANAGER_H
#define TRANSACTION_MANAGER_H

#include "block.h"
#include "space_map.h"

#include <set>
#include <boost/shared_ptr.hpp>

//----------------------------------------------------------------

namespace persistent_data {
	class transaction_manager : boost::noncopyable {
	public:
		typedef boost::shared_ptr<transaction_manager> ptr;
		typedef block_manager<>::read_ref read_ref;
		typedef block_manager<>::write_ref write_ref;
		typedef block_manager<>::validator::ptr validator;

		// If the space map is persistent, then the caller should
		// hold onto a reference and remember to call sm_->commit()
		// and update the superblock before dropping the superblock
		// reference.
		transaction_manager(block_manager<>::ptr bm,
				    space_map::ptr sm);
		~transaction_manager();

		// Drop the superblock reference to commit
		write_ref begin(block_address superblock);
		write_ref begin(block_address superblock, validator v);

		write_ref new_block(validator v);

		// shadowing returns a new write_ref, and a boolean which
		// indicates whether the children should be incremented.
		std::pair<write_ref, bool> shadow(block_address orig);
		std::pair<write_ref, bool> shadow(block_address orig, validator v);

		read_ref read_lock(block_address b);
		read_ref read_lock(block_address b, validator v);

		space_map::ptr get_sm() {
			return sm_;
		}

		void set_sm(space_map::ptr sm) {
			sm_ = sm;
		}

		block_manager<>::ptr get_bm() {
			return bm_;
		}

	private:
		void add_shadow(block_address b);
		void remove_shadow(block_address b);
	        bool is_shadow(block_address b) const;
		void wipe_shadow_table();

		block_manager<>::ptr bm_;
		space_map::ptr sm_;

		std::set<block_address> shadows_;
	};
}

//----------------------------------------------------------------

#endif

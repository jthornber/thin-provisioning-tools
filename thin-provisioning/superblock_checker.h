#ifndef THIN_SUPERBLOCK_CHECKER_H
#define THIN_SUPERBLOCK_CHECKER_H

#include "thin-provisioning/metadata_checker.h"

//----------------------------------------------------------------

namespace thin_provisioning {
	class superblock_checker : public checker {
	public:
		typedef persistent_data::block_manager<> block_manager;

		superblock_checker(block_manager::ptr bm);
		boost::shared_ptr<damage_list> check();

	private:
		block_manager::ptr bm_;
		damage_list_ptr damage;
	};
}

//----------------------------------------------------------------

#endif

#ifndef THIN_SUPERBLOCK_CHECKER_H
#define THIN_SUPERBLOCK_CHECKER_H

#include "thin-provisioning/metadata_checker.h"

//----------------------------------------------------------------

namespace thin_provisioning {
	class superblock_checker : public checker {
	public:
		superblock_checker(block_manager::ptr bm);
		damage_list_ptr check();
	};
}

//----------------------------------------------------------------

#endif

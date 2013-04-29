#ifndef THIN_DEVICE_CHECKER_H
#define THIN_DEVICE_CHECKER_H

#include "thin-provisioning/metadata_checker.h"

//----------------------------------------------------------------

namespace thin_provisioning {
	class device_checker : public checker {
	public:
		device_checker(block_manager::ptr bm);
		damage_list_ptr check();

	private:
		block_manager::ptr bm_;
		damage_list_ptr damage_;
	};
}

//----------------------------------------------------------------

#endif

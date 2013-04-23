#ifndef THIN_SUPERBLOCK_VALIDATOR_H
#define THIN_SUPERBLOCK_VALIDATOR_H

#include "persistent-data/block.h"

//----------------------------------------------------------------

namespace thin_provisioning {
	// FIXME: put with metadata disk structures?
	uint32_t const SUPERBLOCK_MAGIC = 27022010;

	block_manager<>::validator::ptr superblock_validator();
}

//----------------------------------------------------------------

#endif

#include "thin-provisioning/superblock_validator.h"

#include "thin-provisioning/metadata_disk_structures.h"

using namespace thin_provisioning;

//----------------------------------------------------------------

namespace {
        uint32_t const VERSION = 1;
        unsigned const SECTOR_TO_BLOCK_SHIFT = 3;
	uint32_t const SUPERBLOCK_CSUM_SEED = 160774;

	struct validator : public block_manager<>::validator {
		virtual void check(buffer<> const &b, block_address location) const {
			superblock_disk const *sbd = reinterpret_cast<superblock_disk const *>(&b);
			crc32c sum(SUPERBLOCK_CSUM_SEED);
			sum.append(&sbd->flags_, MD_BLOCK_SIZE - sizeof(uint32_t));
			if (sum.get_sum() != to_cpu<uint32_t>(sbd->csum_))
				throw checksum_error("bad checksum in superblock");
		}

		virtual void prepare(buffer<> &b, block_address location) const {
			superblock_disk *sbd = reinterpret_cast<superblock_disk *>(&b);
			crc32c sum(SUPERBLOCK_CSUM_SEED);
			sum.append(&sbd->flags_, MD_BLOCK_SIZE - sizeof(uint32_t));
			sbd->csum_ = to_disk<base::le32>(sum.get_sum());
		}
	};
}

//----------------------------------------------------------------

block_manager<>::validator::ptr
thin_provisioning::superblock_validator()
{
	return block_manager<>::validator::ptr(new validator);
}

//----------------------------------------------------------------

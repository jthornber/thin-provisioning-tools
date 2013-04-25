#include "thin-provisioning/superblock_checker.h"

#include "thin-provisioning/metadata_disk_structures.h"
#include "thin-provisioning/superblock_validator.h"


using namespace thin_provisioning;

//----------------------------------------------------------------

superblock_checker::superblock_checker(block_manager::ptr bm)
	: bm_(bm),
	  damage(new damage_list)
{
}

// FIXME: Other things to check:
// - magic
// - version
// - 3 * flags (should be zero)
// - in bounds: metadata_snap, data_mapping_root
// - metadata_nr_blocks_ matches what we've been given.
damage_list_ptr
superblock_checker::check()
{
	superblock sb;

	damage_list_ptr damage(new damage_list);

	try {
		block_manager::read_ref r = bm_->read_lock(SUPERBLOCK_LOCATION, superblock_validator());
		superblock_disk const *sbd = reinterpret_cast<superblock_disk const *>(&r.data());
		superblock_traits::unpack(*sbd, sb);

	} catch (checksum_error const &e) {
		metadata_damage::ptr err(new super_block_corruption);
		err->set_message("checksum error");
		damage->push_back(err);
		return damage;
	}

	return damage;
}

//----------------------------------------------------------------

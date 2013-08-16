#include "caching/superblock.h"

using namespace caching;
using namespace superblock_detail;

//----------------------------------------------------------------

void
superblock_traits::unpack(superblock_disk const &disk, superblock &core)
{
	core.csum = to_cpu<uint32_t>(disk.csum);
	core.flags = to_cpu<uint32_t>(disk.flags);
	core.blocknr = to_cpu<uint64_t>(disk.blocknr);

	::memcpy(core.uuid, disk.uuid, sizeof(core.uuid));
	core.magic = to_cpu<uint64_t>(disk.magic);
	core.version = to_cpu<uint32_t>(disk.version);

	::memcpy(core.policy_name, disk.policy_name, sizeof(core.policy_name));

	for (unsigned i = 0; i < CACHE_POLICY_VERSION_SIZE; i++)
		core.policy_version[i] = to_cpu<uint32_t>(disk.policy_version[i]);

	core.policy_hint_size = to_cpu<uint32_t>(disk.policy_hint_size);

	::memcpy(core.metadata_space_map_root,
		 disk.metadata_space_map_root,
		 sizeof(core.metadata_space_map_root));

	core.mapping_root = to_cpu<uint64_t>(disk.mapping_root);
	core.hint_root = to_cpu<uint64_t>(disk.hint_root);

	core.discard_root = to_cpu<uint64_t>(disk.discard_root);
	core.discard_block_size = to_cpu<uint64_t>(disk.discard_block_size);
	core.discard_nr_blocks = to_cpu<uint64_t>(disk.discard_nr_blocks);

	core.data_block_size = to_cpu<uint32_t>(disk.data_block_size);
	core.metadata_block_size = to_cpu<uint32_t>(disk.metadata_block_size);
	core.cache_blocks = to_cpu<uint32_t>(disk.cache_blocks);

	core.compat_flags = to_cpu<uint32_t>(disk.compat_flags);
	core.compat_ro_flags = to_cpu<uint32_t>(disk.compat_ro_flags);
	core.incompat_flags = to_cpu<uint32_t>(disk.incompat_flags);

	core.read_hits = to_cpu<uint32_t>(disk.read_hits);
	core.read_misses = to_cpu<uint32_t>(disk.read_misses);
	core.write_hits = to_cpu<uint32_t>(disk.write_hits);
	core.write_misses = to_cpu<uint32_t>(disk.write_misses);
}

void
superblock_traits::pack(superblock const &core, superblock_disk &disk)
{
	disk.csum = to_disk<le32>(core.csum);
	disk.flags = to_disk<le32>(core.flags);
	disk.blocknr = to_disk<le64>(core.blocknr);

	::memcpy(disk.uuid, core.uuid, sizeof(disk.uuid));
	disk.magic = to_disk<le64>(core.magic);
	disk.version = to_disk<le32>(core.version);

	::memcpy(disk.policy_name, core.policy_name, sizeof(disk.policy_name));

	for (unsigned i = 0; i < CACHE_POLICY_VERSION_SIZE; i++)
		disk.policy_version[i] = to_disk<le32>(core.policy_version[i]);

	disk.policy_hint_size = to_disk<le32>(core.policy_hint_size);

	::memcpy(disk.metadata_space_map_root,
		 core.metadata_space_map_root,
		 sizeof(disk.metadata_space_map_root));

	disk.mapping_root = to_disk<le64>(core.mapping_root);
	disk.hint_root = to_disk<le64>(core.hint_root);

	disk.discard_root = to_disk<le64>(core.discard_root);
	disk.discard_block_size = to_disk<le64>(core.discard_block_size);
	disk.discard_nr_blocks = to_disk<le64>(core.discard_nr_blocks);

	disk.data_block_size = to_disk<le32>(core.data_block_size);
	disk.metadata_block_size = to_disk<le32>(core.metadata_block_size);
	disk.cache_blocks = to_disk<le32>(core.cache_blocks);

	disk.compat_flags = to_disk<le32>(core.compat_flags);
	disk.compat_ro_flags = to_disk<le32>(core.compat_ro_flags);
	disk.incompat_flags = to_disk<le32>(core.incompat_flags);

	disk.read_hits = to_disk<le32>(core.read_hits);
	disk.read_misses = to_disk<le32>(core.read_misses);
	disk.write_hits = to_disk<le32>(core.write_hits);
	disk.write_misses = to_disk<le32>(core.write_misses);
}

//--------------------------------

superblock_corruption::superblock_corruption(std::string const &desc)
	: desc_(desc)
{
}

void
superblock_corruption::visit(damage_visitor &v) const
{
	v.visit(*this);
}

//--------------------------------

namespace {
	using namespace persistent_data;
	using namespace superblock_detail;

        uint32_t const VERSION = 1;
        unsigned const SECTOR_TO_BLOCK_SHIFT = 3;
	uint32_t const SUPERBLOCK_CSUM_SEED = 9031977;

	struct sb_validator : public block_manager<>::validator {
		virtual void check(buffer<> const &b, block_address location) const {
			superblock_disk const *sbd = reinterpret_cast<superblock_disk const *>(&b);
			crc32c sum(SUPERBLOCK_CSUM_SEED);
			sum.append(&sbd->flags, MD_BLOCK_SIZE - sizeof(uint32_t));
			if (sum.get_sum() != to_cpu<uint32_t>(sbd->csum))
				throw checksum_error("bad checksum in superblock");
		}

		virtual void prepare(buffer<> &b, block_address location) const {
			superblock_disk *sbd = reinterpret_cast<superblock_disk *>(&b);
			crc32c sum(SUPERBLOCK_CSUM_SEED);
			sum.append(&sbd->flags, MD_BLOCK_SIZE - sizeof(uint32_t));
			sbd->csum = to_disk<base::le32>(sum.get_sum());
		}
	};
}

persistent_data::block_manager<>::validator::ptr
caching::superblock_validator()
{
	return block_manager<>::validator::ptr(new sb_validator);
}

//--------------------------------

superblock_detail::superblock
caching::read_superblock(persistent_data::block_manager<>::ptr bm,
			 persistent_data::block_address location)
{
	using namespace superblock_detail;

	superblock sb;
	block_manager<>::read_ref r = bm->read_lock(location, superblock_validator());
	superblock_disk const *sbd = reinterpret_cast<superblock_disk const *>(&r.data());
	superblock_traits::unpack(*sbd, sb);

	return sb;
}

superblock_detail::superblock
caching::read_superblock(persistent_data::block_manager<>::ptr bm)
{
	return read_superblock(bm, SUPERBLOCK_LOCATION);
}

void
caching::check_superblock(persistent_data::block_manager<>::ptr bm,
			  superblock_detail::damage_visitor &visitor)
{
	using namespace superblock_detail;

	try {
		bm->read_lock(SUPERBLOCK_LOCATION, superblock_validator());

	} catch (std::exception const &e) {
		visitor.visit(superblock_corruption(e.what()));
	}
}

//----------------------------------------------------------------

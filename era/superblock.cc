#include "era/superblock.h"

#include "persistent-data/checksum.h"
#include "persistent-data/errors.h"

using namespace base;
using namespace era;
using namespace persistent_data;

//----------------------------------------------------------------

namespace  {
	using namespace base;

	size_t const SPACE_MAP_ROOT_SIZE = 128;
	size_t const UUID_LEN = 16;

	struct superblock_disk {
		le32 csum;
		le32 flags;
		le64 blocknr;

		__u8 uuid[UUID_LEN];
		le64 magic;
		le32 version;

 		__u8 metadata_space_map_root[SPACE_MAP_ROOT_SIZE];

		le32 data_block_size;
		le32 metadata_block_size;
		le32 nr_blocks;

		le32 current_era;
		era_detail_disk current_detail;

		le64 bloom_tree_root;
		le64 era_array_root;

	} __attribute__ ((packed));

	struct superblock_traits {
		typedef superblock_disk disk_type;
		typedef superblock value_type;

		static void unpack(disk_type const &disk, value_type &value);
		static void pack(value_type const &value, disk_type &disk);
	};

	uint32_t const SUPERBLOCK_MAGIC = 2126579579;
	uint32_t const VERSION_BEGIN = 1;
	uint32_t const VERSION_END = 2;
}

//----------------------------------------------------------------

superblock::superblock()
	: csum(0),
	  blocknr(0),
	  flags(),
	  magic(SUPERBLOCK_MAGIC),
	  version(VERSION_END - 1),
	  data_block_size(0),
	  metadata_block_size(8),
	  nr_blocks(0),
	  current_era(0),
	  bloom_tree_root(0),
	  era_array_root(0)
{
	memset(uuid, 0, sizeof(uuid));
	memset(metadata_space_map_root, 0, sizeof(metadata_space_map_root));
}

//----------------------------------------------------------------

void
superblock_traits::unpack(disk_type const &disk, value_type &value)
{
	//value.flags = to_cpu<uint32_t>(disk.flags);
	value.blocknr = to_cpu<uint64_t>(disk.blocknr);
	value.magic = to_cpu<uint64_t>(disk.magic);
	value.version = to_cpu<uint32_t>(disk.version);

	memcpy(value.metadata_space_map_root, disk.metadata_space_map_root,
	       sizeof(value.metadata_space_map_root));

	value.data_block_size = to_cpu<uint32_t>(disk.data_block_size);
	value.metadata_block_size = to_cpu<uint32_t>(disk.metadata_block_size);
	value.nr_blocks = to_cpu<uint32_t>(disk.nr_blocks);
	value.current_era = to_cpu<uint32_t>(disk.current_era);
	era_detail_traits::unpack(disk.current_detail, value.current_detail);
	value.bloom_tree_root = to_cpu<uint64_t>(disk.bloom_tree_root);
	value.era_array_root = to_cpu<uint64_t>(disk.era_array_root);
}

void
superblock_traits::pack(value_type const &value, disk_type &disk)
{
	//disk.flags = to_disk<uint32_t>(value.flags);
	disk.blocknr = to_disk<le64>(value.blocknr);
	disk.magic = to_disk<le64>(value.magic);
	disk.version = to_disk<le32>(value.version);

	memcpy(disk.metadata_space_map_root, value.metadata_space_map_root,
	       sizeof(disk.metadata_space_map_root));

	disk.data_block_size = to_disk<le32>(value.data_block_size);
	disk.metadata_block_size = to_disk<le32>(value.metadata_block_size);
	disk.nr_blocks = to_disk<le32>(value.nr_blocks);
	disk.current_era = to_disk<le32>(value.current_era);
	era_detail_traits::pack(value.current_detail, disk.current_detail);
	disk.bloom_tree_root = to_disk<le64>(value.bloom_tree_root);
	disk.era_array_root = to_disk<le64>(value.era_array_root);
}

//----------------------------------------------------------------

namespace validator {
	using namespace persistent_data;

        uint32_t const VERSION = 1;
        unsigned const SECTOR_TO_BLOCK_SHIFT = 3;
	uint32_t const SUPERBLOCK_CSUM_SEED = 146538381;

	// FIXME: turn into a template, we have 3 similar classes now
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

	block_manager<>::validator::ptr  mk_v() {
		return block_manager<>::validator::ptr(new sb_validator);
	}
}

//----------------------------------------------------------------

superblock
era::read_superblock(block_manager<>::ptr bm, block_address location)
{
	superblock sb;
	block_manager<>::read_ref r = bm->read_lock(location, validator::mk_v());
	superblock_disk const *sbd = reinterpret_cast<superblock_disk const *>(&r.data());
	superblock_traits::unpack(*sbd, sb);

	return sb;
}

void
era::write_superblock(block_manager<>::ptr bm, superblock const &sb, block_address location)
{
	block_manager<>::write_ref w = bm->superblock_zero(location, validator::mk_v());
	superblock_traits::pack(sb, *reinterpret_cast<superblock_disk *>(w.data().raw()));
}

//----------------------------------------------------------------

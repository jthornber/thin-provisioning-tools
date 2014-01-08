#include "era/superblock.h"

#include "persistent-data/checksum.h"

using namespace base;
using namespace era;
using namespace persistent_data;

//----------------------------------------------------------------

namespace  {
	using namespace base;

	struct superblock_disk {
		le32 csum;
		le32 flags;
		le64 blocknr;

		__u8 uuid[16];
		le64 magic;
		le32 version;

		le32 data_block_size;
		le32 metadata_block_size;
		le32 nr_blocks;

		le32 current_era;
		era_detail_disk current_detail;

		le64 bloom_filters_root;
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
	  era_root(0),
	  era_array_root(0)
{
	memset(uuid, 0, sizeof(uuid));
	memset(metadata_space_map_root, 0, sizeof(metadata_space_map_root));
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

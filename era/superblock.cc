#include "era/superblock.h"

#include "persistent-data/checksum.h"
#include "persistent-data/errors.h"

using namespace base;
using namespace era;
using namespace superblock_damage;
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

		le64 writeset_tree_root;
		le64 era_array_root;

		le64 metadata_snap;

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

superblock_flags::superblock_flags()
	: unhandled_flags_(0)
{
}

superblock_flags::superblock_flags(uint32_t bits)
{
	if (bits & (1 << CLEAN_SHUTDOWN_BIT)) {
		flags_.insert(CLEAN_SHUTDOWN);
		bits &= ~(1 << CLEAN_SHUTDOWN_BIT);
	}

	unhandled_flags_ = bits;
}

void
superblock_flags::set_flag(flag f)
{
	flags_.insert(f);
}

void
superblock_flags::clear_flag(flag f)
{
	flags_.erase(f);
}

bool
superblock_flags::get_flag(flag f) const
{
	return flags_.find(f) != flags_.end();
}

uint32_t
superblock_flags::encode() const
{
	uint32_t r = 0;

	if (get_flag(CLEAN_SHUTDOWN))
		r = r | (1 << CLEAN_SHUTDOWN_BIT);

	return r;
}

uint32_t
superblock_flags::get_unhandled_flags() const
{
	return unhandled_flags_;
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
	  writeset_tree_root(0),
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
	value.writeset_tree_root = to_cpu<uint64_t>(disk.writeset_tree_root);
	value.era_array_root = to_cpu<uint64_t>(disk.era_array_root);

	block_address ms = to_cpu<uint64_t>(disk.metadata_snap);
	value.metadata_snap = (ms == SUPERBLOCK_LOCATION) ?
		boost::optional<block_address>() :
		boost::optional<block_address>(ms);
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
	disk.writeset_tree_root = to_disk<le64>(value.writeset_tree_root);
	disk.era_array_root = to_disk<le64>(value.era_array_root);

	disk.metadata_snap = value.metadata_snap ?
		to_disk<le64>(*value.metadata_snap) :
		to_disk<le64>(SUPERBLOCK_LOCATION);
}

//--------------------------------

superblock_corrupt::superblock_corrupt(std::string const &desc)
	: damage(desc)
{
}

void
superblock_corrupt::visit(damage_visitor &v) const
{
	v.visit(*this);
}

superblock_invalid::superblock_invalid(std::string const &desc)
	: damage(desc)
{
}

void
superblock_invalid::visit(damage_visitor &v) const
{
	v.visit(*this);
}

//----------------------------------------------------------------

namespace era_validator {
	using namespace persistent_data;

        uint32_t const VERSION = 1;
        unsigned const SECTOR_TO_BLOCK_SHIFT = 3;
	uint32_t const SUPERBLOCK_CSUM_SEED = 146538381;

	// FIXME: turn into a template, we have 3 similar classes now
	struct sb_validator : public bcache::validator {
		virtual void check(void const *raw, block_address location) const {
			superblock_disk const *sbd = reinterpret_cast<superblock_disk const *>(raw);
			crc32c sum(SUPERBLOCK_CSUM_SEED);
			sum.append(&sbd->flags, MD_BLOCK_SIZE - sizeof(uint32_t));
			if (sum.get_sum() != to_cpu<uint32_t>(sbd->csum))
				throw checksum_error("bad checksum in superblock");
		}

		virtual void prepare(void *raw, block_address location) const {
			superblock_disk *sbd = reinterpret_cast<superblock_disk *>(raw);
			crc32c sum(SUPERBLOCK_CSUM_SEED);
			sum.append(&sbd->flags, MD_BLOCK_SIZE - sizeof(uint32_t));
			sbd->csum = to_disk<base::le32>(sum.get_sum());
		}
	};

	bcache::validator::ptr  mk_v() {
		return bcache::validator::ptr(new sb_validator);
	}
}

//----------------------------------------------------------------

superblock
era::read_superblock(block_manager<>::ptr bm, block_address location)
{
	superblock sb;
	block_manager<>::read_ref r = bm->read_lock(location, era_validator::mk_v());
	superblock_disk const *sbd = reinterpret_cast<superblock_disk const *>(r.data());
	superblock_traits::unpack(*sbd, sb);

	return sb;
}

void
era::write_superblock(block_manager<>::ptr bm, superblock const &sb, block_address location)
{
	block_manager<>::write_ref w = bm->superblock_zero(location, era_validator::mk_v());
	superblock_traits::pack(sb, *reinterpret_cast<superblock_disk *>(w.data()));
}

void
era::check_superblock(superblock const &sb,
		      block_address nr_metadata_blocks,
		      damage_visitor &visitor)
{
	if (sb.flags.get_unhandled_flags()) {
		ostringstream msg;
		msg << "invalid flags: " << sb.flags.get_unhandled_flags();
		visitor.visit(superblock_invalid(msg.str()));
	}

	if (sb.blocknr >= nr_metadata_blocks) {
		ostringstream msg;
		msg << "blocknr out of bounds: " << sb.blocknr << " >= " << nr_metadata_blocks;
		visitor.visit(superblock_invalid(msg.str()));
	}

	if (sb.magic != SUPERBLOCK_MAGIC) {
		ostringstream msg;
		msg << "magic in incorrect: " << sb.magic;
		visitor.visit(superblock_invalid(msg.str()));
	}

	if (sb.version >= VERSION_END) {
		ostringstream msg;
		msg << "version incorrect: " << sb.version;
		visitor.visit(superblock_invalid(msg.str()));
	}

	if (sb.version < VERSION_BEGIN) {
		ostringstream msg;
		msg << "version incorrect: " << sb.version;
		visitor.visit(superblock_invalid(msg.str()));
	}

	if (sb.metadata_block_size != 8) {
		ostringstream msg;
		msg << "metadata block size incorrect: " << sb.metadata_block_size;
		visitor.visit(superblock_invalid(msg.str()));
	}

	if (sb.writeset_tree_root == SUPERBLOCK_LOCATION) {
		string msg("writeset tree root points back to the superblock");
		visitor.visit(superblock_invalid(msg));
	}

	if (sb.era_array_root == SUPERBLOCK_LOCATION) {
		string msg("era array root points back to the superblock");
		visitor.visit(superblock_invalid(msg));
	}

	if (sb.writeset_tree_root == sb.era_array_root) {
		ostringstream msg;
		msg << "writeset tree root and era array both point to the same block: "
		    << sb.era_array_root;
		visitor.visit(superblock_invalid(msg.str()));
	}
}

void
era::check_superblock(persistent_data::block_manager<>::ptr bm,
		      block_address nr_metadata_blocks,
		      damage_visitor &visitor)
{
	superblock sb;

	try {
		sb = read_superblock(bm, SUPERBLOCK_LOCATION);

	} catch (std::exception const &e) {

		// FIXME: what if it fails due to a zero length file?  Not
		// really a corruption, so much as an io error.  Should we
		// separate these?

		visitor.visit(superblock_corrupt(e.what()));
	}

	check_superblock(sb, nr_metadata_blocks, visitor);
}


//----------------------------------------------------------------

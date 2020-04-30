#include "base/bits.h"
#include "caching/superblock.h"

using namespace base;
using namespace caching;
using namespace superblock_damage;

//----------------------------------------------------------------

namespace {
	using namespace base;

	struct superblock_disk {
		le32 csum;
		le32 flags;
		le64 blocknr;

		__u8 uuid[16];
		le64 magic;
		le32 version;

		__u8 policy_name[CACHE_POLICY_NAME_SIZE];
		le32 policy_hint_size;

		__u8 metadata_space_map_root[SPACE_MAP_ROOT_SIZE];

		le64 mapping_root;
		le64 hint_root;

		le64 discard_root;
		le64 discard_block_size;
		le64 discard_nr_blocks;

		le32 data_block_size; /* in 512-byte sectors */
		le32 metadata_block_size; /* in 512-byte sectors */
		le32 cache_blocks;

		le32 compat_flags;
		le32 compat_ro_flags;
		le32 incompat_flags;

		le32 read_hits;
		le32 read_misses;
		le32 write_hits;
		le32 write_misses;

		le32 policy_version[CACHE_POLICY_VERSION_SIZE];

		le64 dirty_root; // format 2 only
	} __attribute__ ((packed));

	struct superblock_traits {
		typedef superblock_disk disk_type;
		typedef superblock value_type;

		static void unpack(superblock_disk const &disk, superblock &value);
		static void pack(superblock const &value, superblock_disk &disk);
	};

	uint32_t const SUPERBLOCK_MAGIC = 06142003;
	uint32_t const VERSION_BEGIN = 1;
	uint32_t const VERSION_END = 3;
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

	if (bits & (1u << NEEDS_CHECK_BIT)) {
		flags_.insert(NEEDS_CHECK);
		bits &= ~(1u << NEEDS_CHECK_BIT);
	}

	unhandled_flags_ = bits;
}

void
superblock_flags::set_flag(superblock_flags::flag f)
{
	flags_.insert(f);
}

void
superblock_flags::clear_flag(superblock_flags::flag f)
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

	if (get_flag(NEEDS_CHECK))
		r = r | (1u << NEEDS_CHECK_BIT);

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
	  blocknr(SUPERBLOCK_LOCATION),
	  magic(SUPERBLOCK_MAGIC),
	  version(VERSION_END - 1u),
	  policy_hint_size(4),
	  mapping_root(0),
	  hint_root(0),
	  discard_root(0),
	  discard_block_size(0),
	  discard_nr_blocks(0),
	  data_block_size(0),
	  metadata_block_size(8),
	  cache_blocks(0),
	  compat_flags(0),
	  compat_ro_flags(0),
	  incompat_flags(0),
	  read_hits(0),
	  read_misses(0),
	  write_hits(0),
	  write_misses(0)
{
	::memset(uuid, 0, sizeof(uuid));
	::memset(policy_name, 0, sizeof(policy_name));
	::memset(policy_version, 0, sizeof(policy_version));
	::memset(metadata_space_map_root, 0, sizeof(metadata_space_map_root));
}

//----------------------------------------------------------------

void
superblock_traits::unpack(superblock_disk const &disk, superblock &core)
{
	core.compat_flags = to_cpu<uint32_t>(disk.compat_flags);
	core.compat_ro_flags = to_cpu<uint32_t>(disk.compat_ro_flags);
	core.incompat_flags = to_cpu<uint32_t>(disk.incompat_flags);

	core.csum = to_cpu<uint32_t>(disk.csum);

	core.flags = superblock_flags(to_cpu<uint32_t>(disk.flags));
	core.blocknr = to_cpu<uint64_t>(disk.blocknr);

	::memcpy(core.uuid, disk.uuid, sizeof(core.uuid));
	core.magic = to_cpu<uint64_t>(disk.magic);
	core.version = to_cpu<uint32_t>(disk.version);

	::memcpy(core.policy_name, disk.policy_name, sizeof(core.policy_name));

	for (unsigned i = 0; i < CACHE_POLICY_VERSION_SIZE; i++)
		core.policy_version[i] = to_cpu<uint32_t>(disk.policy_version[i]);

	core.policy_hint_size = test_bit(core.incompat_flags, VARIABLE_HINT_SIZE_BIT) ?
		to_cpu<uint32_t>(disk.policy_hint_size) : 4;

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

	core.read_hits = to_cpu<uint32_t>(disk.read_hits);
	core.read_misses = to_cpu<uint32_t>(disk.read_misses);
	core.write_hits = to_cpu<uint32_t>(disk.write_hits);
	core.write_misses = to_cpu<uint32_t>(disk.write_misses);

	if (core.version >= 2)
		core.dirty_root = to_cpu<uint64_t>(disk.dirty_root);
}

void
superblock_traits::pack(superblock const &sb, superblock_disk &disk)
{
	// We adjust some of the flags in the superblock, so make a copy
	superblock core(sb);

	disk.csum = to_disk<le32>(core.csum);
	disk.flags = to_disk<le32>(core.flags.encode());
	disk.blocknr = to_disk<le64>(core.blocknr);

	::memcpy(disk.uuid, core.uuid, sizeof(disk.uuid));
	disk.magic = to_disk<le64>(core.magic);
	disk.version = to_disk<le32>(core.version);

	::memcpy(disk.policy_name, core.policy_name, sizeof(disk.policy_name));

	for (unsigned i = 0; i < CACHE_POLICY_VERSION_SIZE; i++)
		disk.policy_version[i] = to_disk<le32>(core.policy_version[i]);

	if (core.policy_hint_size != 4) {
		set_bit(core.incompat_flags, VARIABLE_HINT_SIZE_BIT);
		disk.policy_hint_size = to_disk<le32>(core.policy_hint_size);
	} else {
		clear_bit(core.incompat_flags, VARIABLE_HINT_SIZE_BIT);
		disk.policy_hint_size = to_disk<le32>(0u);
	}

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

	// The version may be overridden, meaning the dirty root may not
	// actually be present.
	if (core.version >= 2 && core.dirty_root)
		disk.dirty_root = to_disk<le64>(*core.dirty_root);
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

//--------------------------------

// anonymous namespace doesn't work for some reason
namespace validator {
	using namespace persistent_data;

	uint32_t const SUPERBLOCK_CSUM_SEED = 9031977;

	struct sb_validator : public bcache::validator {
		virtual void check(void const *raw, block_address location) const {
			superblock_disk const *sbd = reinterpret_cast<superblock_disk const *>(raw);
			crc32c sum(SUPERBLOCK_CSUM_SEED);
			sum.append(&sbd->flags, MD_BLOCK_SIZE - sizeof(uint32_t));
			if (sum.get_sum() != to_cpu<uint32_t>(sbd->csum))
				throw checksum_error("bad checksum in superblock");
		}

		virtual bool check_raw(void const *raw) const {
			superblock_disk const *sbd = reinterpret_cast<superblock_disk const *>(raw);
			crc32c sum(SUPERBLOCK_CSUM_SEED);
			sum.append(&sbd->flags, MD_BLOCK_SIZE - sizeof(uint32_t));
			if (sum.get_sum() != to_cpu<uint32_t>(sbd->csum))
				return false;
			return true;
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

//--------------------------------

superblock
caching::read_superblock(block_manager::ptr bm, block_address location)
{
	using namespace validator;
	superblock sb;
	block_manager::read_ref r = bm->read_lock(location, mk_v());
	superblock_disk const *sbd = reinterpret_cast<superblock_disk const *>(r.data());
	superblock_traits::unpack(*sbd, sb);

	return sb;
}

void
caching::write_superblock(block_manager::ptr bm, superblock const &sb, block_address location)
{
	using namespace validator;
	block_manager::write_ref w = bm->superblock_zero(location, mk_v());
	superblock_traits::pack(sb, *reinterpret_cast<superblock_disk *>(w.data()));
}

void
caching::check_superblock(superblock const &sb,
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
		msg << "magic is incorrect: " << sb.magic;
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

	if (::strnlen((char const *) sb.policy_name, CACHE_POLICY_NAME_SIZE) == CACHE_POLICY_NAME_SIZE) {
		visitor.visit(superblock_invalid("policy name is not null terminated"));
	}

	if (sb.policy_hint_size % 4 || sb.policy_hint_size > 128) {
		ostringstream msg;
		msg << "policy hint size invalid: " << sb.policy_hint_size;
		visitor.visit(superblock_invalid(msg.str()));
	}

	if (sb.metadata_block_size != 8) {
		ostringstream msg;
		msg << "metadata block size incorrect: " << sb.metadata_block_size;
		visitor.visit(superblock_invalid(msg.str()));
	}

	if (sb.compat_flags != 0) {
		ostringstream msg;
		msg << "compat_flags invalid (can only be 0): " << sb.compat_flags;
		visitor.visit(superblock_invalid(msg.str()));
	}

	if (sb.compat_ro_flags != 0) {
		ostringstream msg;
		msg << "compat_ro_flags invalid (can only be 0): " << sb.compat_ro_flags;
		visitor.visit(superblock_invalid(msg.str()));
	}

	if (sb.incompat_flags & ~(1 << VARIABLE_HINT_SIZE_BIT)) {
		ostringstream msg;
		msg << "incompat_flags invalid (can only be 0): " << sb.incompat_flags;
		visitor.visit(superblock_invalid(msg.str()));
	}
}

void
caching::check_superblock(persistent_data::block_manager::ptr bm,
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

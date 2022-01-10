#include "caching/metadata.h"
#include "caching/superblock.h"
#include "persistent-data/space-maps/core.h"

using namespace caching;
namespace pd = persistent_data;

//----------------------------------------------------------------

namespace {
	// FIXME: duplication
	transaction_manager::ptr
	open_tm(block_manager::ptr bm) {
		auto nr_blocks = bm->get_nr_blocks();
		if (!nr_blocks)
			throw runtime_error("Metadata is not large enough for superblock.");

		space_map::ptr sm{create_core_map(nr_blocks)};
		sm->inc(SUPERBLOCK_LOCATION);
		transaction_manager::ptr tm(new transaction_manager(bm, sm));
		return tm;
	}

	void
	copy_space_maps(space_map::ptr lhs, space_map::ptr rhs) {
		for (block_address b = 0; b < rhs->get_nr_blocks(); b++) {
			uint32_t count = rhs->get_count(b);
			if (count > 0)
				lhs->set_count(b, rhs->get_count(b));
		}
	}
}

//----------------------------------------------------------------

metadata::metadata(block_manager::ptr bm, open_type ot, unsigned metadata_version)
{
	switch (ot) {
	case CREATE:
		create_metadata(bm, metadata_version);
		break;

	default:
		throw runtime_error("unhandled open_type");
	}
}

metadata::metadata(block_manager::ptr bm, bool read_space_map)
{
	open_metadata(bm, read_space_map);
}

void
metadata::commit(bool clean_shutdown)
{
	commit_space_map();
	commit_mappings();
	commit_hints();
	commit_discard_bits();
	commit_superblock(clean_shutdown);
}

void
metadata::setup_hint_array(size_t width)
{
	if (width > 0)
		hints_ = hint_array::ptr(
			new hint_array(*tm_, width));
}

void
metadata::create_metadata(block_manager::ptr bm, unsigned metadata_version)
{
	tm_ = open_tm(bm);

	space_map::ptr core = tm_->get_sm();
	metadata_sm_ = create_metadata_sm(*tm_, tm_->get_bm()->get_nr_blocks());
	copy_space_maps(metadata_sm_, core);
	tm_->set_sm(metadata_sm_);

	mappings_ = mapping_array::ptr(new mapping_array(*tm_, mapping_array::ref_counter()));

	// We can't instantiate the hint array yet, since we don't know the
	// hint width.

	discard_bits_ = pd::bitset::ptr(new pd::bitset(*tm_));

	if (metadata_version >= 2)
		dirty_bits_ = pd::bitset::ptr(new pd::bitset(*tm_));
}

void
metadata::open_metadata(block_manager::ptr bm, bool read_space_map)
{
	tm_ = open_tm(bm);
	sb_ = read_superblock(tm_->get_bm());

	if (read_space_map) {
		metadata_sm_ = open_metadata_sm(*tm_, &sb_.metadata_space_map_root);
		tm_->set_sm(metadata_sm_);
	}

	mappings_ = mapping_array::ptr(
		new mapping_array(*tm_,
				  mapping_array::ref_counter(),
				  sb_.mapping_root,
				  sb_.cache_blocks));

	if (sb_.hint_root)
		hints_ = hint_array::ptr(
			new hint_array(*tm_, sb_.policy_hint_size,
				       sb_.hint_root, sb_.cache_blocks));

	if (sb_.discard_root)
		discard_bits_ = pd::bitset::ptr(
			new pd::bitset(*tm_, sb_.discard_root, sb_.discard_nr_blocks));

	if (sb_.version >= 2)
		dirty_bits_ = pd::bitset::ptr(
			new pd::bitset(*tm_, *sb_.dirty_root, sb_.cache_blocks));
}

void
metadata::commit_space_map()
{
	metadata_sm_->commit();
	metadata_sm_->copy_root(&sb_.metadata_space_map_root, sizeof(sb_.metadata_space_map_root));
}

void
metadata::commit_mappings()
{
	sb_.mapping_root = mappings_->get_root();
	if (sb_.version >= 2) {
		dirty_bits_->flush();
		sb_.dirty_root = dirty_bits_->get_root();
	}
}

void
metadata::commit_hints()
{
	sb_.hint_root = hints_->get_root();
}

void
metadata::commit_discard_bits()
{
	sb_.discard_root = discard_bits_->get_root();
}

void
metadata::commit_superblock(bool clean_shutdown)
{
	if (clean_shutdown)
		sb_.flags.set_flag(superblock_flags::CLEAN_SHUTDOWN);

	write_superblock(tm_->get_bm(), sb_);

	sb_.flags.clear_flag(superblock_flags::CLEAN_SHUTDOWN);
}

//----------------------------------------------------------------

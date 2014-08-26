#include "era/metadata.h"
#include "persistent-data/space-maps/core.h"

using namespace era;

//----------------------------------------------------------------

namespace {
	unsigned const METADATA_CACHE_SIZ = 1024;

	// FIXME: duplication
	transaction_manager::ptr
	open_tm(block_manager<>::ptr bm) {
		space_map::ptr sm(new core_map(bm->get_nr_blocks()));
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

metadata::metadata(block_manager<>::ptr bm, open_type ot)
{
	switch (ot) {
	case CREATE:
		create_metadata(bm);
		break;

	case OPEN:
		open_metadata(bm);
		break;
	}
}

metadata::metadata(block_manager<>::ptr bm, block_address metadata_snap)
{
	open_metadata(bm);
}

void
metadata::create_metadata(block_manager<>::ptr bm)
{
	tm_ = open_tm(bm);

	space_map::ptr core = tm_->get_sm();
	metadata_sm_ = create_metadata_sm(*tm_, tm_->get_bm()->get_nr_blocks());
	copy_space_maps(metadata_sm_, core);
	tm_->set_sm(metadata_sm_);

	writeset_tree_ = writeset_tree::ptr(new writeset_tree(*tm_, era_detail_traits::ref_counter(tm_)));
	era_array_ = era_array::ptr(new era_array(*tm_,
						  uint32_traits::ref_counter()));
}

void
metadata::open_metadata(block_manager<>::ptr bm, block_address loc)
{
	tm_ = open_tm(bm);
	sb_ = read_superblock(tm_->get_bm(), loc);

	writeset_tree_ = writeset_tree::ptr(new writeset_tree(*tm_,
							      sb_.writeset_tree_root,
							      era_detail_traits::ref_counter(tm_)));

	era_array_ = era_array::ptr(new era_array(*tm_,
						  uint32_traits::ref_counter(),
						  sb_.era_array_root,
						  sb_.nr_blocks));
}

void
metadata::commit()
{
	commit_space_map();
	commit_writesets();
	commit_era_array();
	commit_superblock();
}

void
metadata::commit_space_map()
{
	metadata_sm_->commit();
	metadata_sm_->copy_root(&sb_.metadata_space_map_root, sizeof(sb_.metadata_space_map_root));
}

void
metadata::commit_writesets()
{
	sb_.writeset_tree_root = writeset_tree_->get_root();
}

void
metadata::commit_era_array()
{
	sb_.era_array_root = era_array_->get_root();
}

void
metadata::commit_superblock()
{
	write_superblock(tm_->get_bm(), sb_);
}

//----------------------------------------------------------------

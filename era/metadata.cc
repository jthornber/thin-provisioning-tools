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
		// finish
		throw runtime_error("not imlemented");
		break;

	case OPEN:
		open_metadata(bm);
		break;
	}
}

void
metadata::open_metadata(block_manager<>::ptr bm)
{
	tm_ = open_tm(bm);
	sb_ = read_superblock(tm_->get_bm());

	bloom_tree_ = bloom_tree::ptr(new bloom_tree(tm_,
						     sb_.bloom_tree_root,
						     era_detail_traits::ref_counter(tm_)));

	era_array_ = era_array::ptr(new era_array(tm_,
						  uint32_traits::ref_counter(),
						  sb_.era_array_root,
						  sb_.nr_blocks));
}

//----------------------------------------------------------------

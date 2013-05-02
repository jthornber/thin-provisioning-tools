#include "thin-provisioning/device_checker.h"

#include "persistent-data/data-structures/btree_checker.h"
#include "persistent-data/transaction_manager.h"
#include "persistent-data/space-maps/core.h"
#include "thin-provisioning/metadata.h"
#include "thin-provisioning/metadata_disk_structures.h"

using namespace persistent_data;
using namespace thin_provisioning;

//----------------------------------------------------------------

namespace {
	// FIXME: duplication with metadata.cc
	transaction_manager::ptr
	open_core_tm(block_manager<>::ptr bm) {
		space_map::ptr sm(new core_map(bm->get_nr_blocks()));
		sm->inc(SUPERBLOCK_LOCATION);
		transaction_manager::ptr tm(new transaction_manager(bm, sm));
		return tm;
	}

	class device_visitor : public btree<1, device_details_traits>::visitor {
	public:
		typedef boost::shared_ptr<device_visitor> ptr;
		typedef btree_checker<1, device_details_traits> checker;

		device_visitor(block_counter &counter)
			: checker_(counter) {
		}

		bool visit_internal(node_location const &loc,
				    btree_detail::node_ref<uint64_traits> const &n) {
			return checker_.visit_internal(loc, n);
		}

		bool visit_internal_leaf(node_location const &loc,
					 btree_detail::node_ref<uint64_traits> const &n) {
			return checker_.visit_internal_leaf(loc, n);
		}

		bool visit_leaf(node_location const &loc,
				btree_detail::node_ref<device_details_traits> const &n) {

			if (!checker_.visit_leaf(loc, n))
				return false;

			for (unsigned i = 0; i < n.get_nr_entries(); i++)
				devices_.insert(n.key_at(i));

			return true;
		}

		set<uint64_t> const &get_devices() const {
			return devices_;
		}

	private:
		checker checker_;
		set<uint64_t> devices_;
	};
}

//----------------------------------------------------------------

device_checker::device_checker(block_manager::ptr bm,
			       block_address root)
	: checker(bm),
	  root_(root)
{
}

damage_list_ptr
device_checker::check()
{
	block_counter counter;
	device_visitor v(counter);
	transaction_manager::ptr tm(open_core_tm(bm_));
	detail_tree::ptr details(new detail_tree(tm, root_,
						 device_details_traits::ref_counter()));
	damage_list_ptr damage(new damage_list);

	try {
		details->visit_depth_first(v);

	} catch (std::exception const &e) {

		metadata_damage::ptr d(new missing_device_details(range64()));
		damage->push_back(d);
	}

	return damage;
}

//----------------------------------------------------------------

#include "thin-provisioning/device_tree.h"

#include "persistent-data/data-structures/btree_damage_visitor.h"
#include "persistent-data/transaction_manager.h"
#include "persistent-data/space-maps/core.h"
#include "thin-provisioning/metadata.h"
#include "thin-provisioning/metadata_disk_structures.h"

using namespace persistent_data;
using namespace thin_provisioning;

//----------------------------------------------------------------

namespace {
	using namespace device_tree_detail;

	// No op for now, should add sanity checks in here however.
	struct leaf_visitor {
		virtual void visit(btree_path const &path, device_details const &dd) {
		}
	};

	class ll_damage_visitor {
	public:
		ll_damage_visitor(device_tree_detail::damage_visitor &v)
		: v_(v) {
		}

		virtual void visit(btree_path const &path, btree_detail::damage const &d) {
			v_.visit(path, missing_devices(d.desc_, d.lost_keys_));
		}

	private:
		device_tree_detail::damage_visitor &v_;
	};
}

namespace thin_provisioning {
	namespace device_tree_detail {
		missing_devices::missing_devices(std::string const &desc,
						 range<uint64_t> const &keys)
			: desc_(desc),
			  keys_(keys) {
		}

		void missing_devices::visit(damage_visitor &v) const {
			v.visit(*this);
		}
	}

	void check_device_tree(device_tree const &tree, damage_visitor &visitor)
	{
		block_counter counter;	// FIXME: get rid of this counter arg
		leaf_visitor vv;
		ll_damage_visitor dv(visitor);

		btree_visit_values(tree, counter, vv, dv);
	}
}

//----------------------------------------------------------------

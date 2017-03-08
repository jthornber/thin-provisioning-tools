#include "thin-provisioning/device_tree.h"

#include "persistent-data/data-structures/btree_damage_visitor.h"

using namespace persistent_data;
using namespace thin_provisioning;

//----------------------------------------------------------------

namespace {
	using namespace device_tree_detail;

	struct visitor_adapter {
		visitor_adapter(device_visitor &dv)
		: dv_(dv) {
		}

		void visit(btree_path const &path, device_details const &dd) {
			dv_.visit(path[0], dd);
		}

	private:
		device_visitor &dv_;
	};

	// No op for now, should add sanity checks in here however.
	struct noop_visitor : public device_visitor {
		virtual void visit(block_address dev_id, device_details const &dd) {
		}
	};

	class ll_damage_visitor {
	public:
		// FIXME: is the namespace needed on here?
		ll_damage_visitor(device_tree_detail::damage_visitor &v)
		: v_(v) {
		}

		virtual void visit(btree_path const &path, btree_detail::damage const &d) {
			v_.visit(missing_devices(d.desc_, d.lost_keys_));
		}

	private:
		device_tree_detail::damage_visitor &v_;
	};
}

namespace thin_provisioning {
	namespace device_tree_detail {
		device_details::device_details()
			: mapped_blocks_(0),
			  transaction_id_(0),
			  creation_time_(0),
			  snapshotted_time_(0) {
		}

		void
		device_details_traits::unpack(device_details_disk const &disk, device_details &value)
		{
			value.mapped_blocks_ = to_cpu<uint64_t>(disk.mapped_blocks_);
			value.transaction_id_ = to_cpu<uint64_t>(disk.transaction_id_);
			value.creation_time_ = to_cpu<uint32_t>(disk.creation_time_);
			value.snapshotted_time_ = to_cpu<uint32_t>(disk.snapshotted_time_);
		}

		void
		device_details_traits::pack(device_details const &value, device_details_disk &disk)
		{
			disk.mapped_blocks_ = to_disk<le64>(value.mapped_blocks_);
			disk.transaction_id_ = to_disk<le64>(value.transaction_id_);
			disk.creation_time_ = to_disk<le32>(value.creation_time_);
			disk.snapshotted_time_ = to_disk<le32>(value.snapshotted_time_);
		}

		missing_devices::missing_devices(std::string const &desc,
						 run<uint64_t> const &keys)
			: desc_(desc),
			  keys_(keys) {
		}

		void missing_devices::visit(damage_visitor &v) const {
			v.visit(*this);
		}
	}
}

//----------------------------------------------------------------

void
thin_provisioning::walk_device_tree(device_tree const &tree,
				    device_tree_detail::device_visitor &vv,
				    device_tree_detail::damage_visitor &dv)
{
	visitor_adapter av(vv);
	ll_damage_visitor ll_dv(dv);
	btree_visit_values(tree, av, ll_dv);
}

void
thin_provisioning::check_device_tree(device_tree const &tree, damage_visitor &visitor)
{
	noop_visitor vv;
	walk_device_tree(tree, vv, visitor);
}

//----------------------------------------------------------------

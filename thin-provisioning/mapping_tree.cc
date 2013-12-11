#include "thin-provisioning/mapping_tree.h"

#include "persistent-data/data-structures/btree_damage_visitor.h"
#include "persistent-data/space_map.h"

using namespace persistent_data;

//----------------------------------------------------------------

namespace thin_provisioning {
	namespace mapping_tree_detail {
		space_map_ref_counter::space_map_ref_counter(space_map::ptr sm)
			: sm_(sm)
		{
		}

		void
		space_map_ref_counter::inc(block_address b)
		{
			sm_->inc(b);
		}

		void
		space_map_ref_counter::dec(block_address b)
		{
			sm_->dec(b);
		}

		//--------------------------------

		block_time_ref_counter::block_time_ref_counter(space_map::ptr sm)
			: sm_(sm)
		{
		}

		void
		block_time_ref_counter::inc(block_time bt)
		{
			sm_->inc(bt.block_);
		}

		void
		block_time_ref_counter::dec(block_time bt)
		{
			sm_->dec(bt.block_);
		}

		//--------------------------------

		void
		block_traits::unpack(disk_type const &disk, value_type &value)
		{
			uint64_t v = to_cpu<uint64_t>(disk);
			value.block_ = v >> 24;
			value.time_ = v & ((1 << 24) - 1);
		}

		void
		block_traits::pack(value_type const &value, disk_type &disk)
		{
			uint64_t v = (value.block_ << 24) | value.time_;
			disk = base::to_disk<base::le64>(v);
		}

		//--------------------------------

		mtree_ref_counter::mtree_ref_counter(transaction_manager::ptr tm)
			: tm_(tm)
		{
		}

		void
		mtree_ref_counter::inc(block_address b)
		{
		}

		void
		mtree_ref_counter::dec(block_address b)
		{
		}

		//--------------------------------

		void
		mtree_traits::unpack(disk_type const &disk, value_type &value)
		{
			value = base::to_cpu<uint64_t>(disk);
		}

		void
		mtree_traits::pack(value_type const &value, disk_type &disk)
		{
			disk = base::to_disk<base::le64>(value);
		}

		//--------------------------------

		missing_devices::missing_devices(std::string const &desc, run<uint64_t> const &keys)
		: desc_(desc),
		  keys_(keys)
		{
		}

		void
		missing_devices::visit(damage_visitor &v) const
		{
			v.visit(*this);
		}

		//--------------------------------

		missing_mappings::missing_mappings(std::string const &desc, uint64_t thin_dev,
						   run<uint64_t> const &keys)
			: desc_(desc),
			thin_dev_(thin_dev),
			keys_(keys)
		{
		}

		void
		missing_mappings::visit(damage_visitor &v) const
		{
			v.visit(*this);
		}
	}
}

//----------------------------------------------------------------

namespace {
	using namespace thin_provisioning;
	using namespace mapping_tree_detail;

	struct noop_block_time_visitor : public mapping_tree_detail::mapping_visitor {
		virtual void visit(btree_path const &, block_time const &) {
		}
	};

	struct noop_block_visitor : public mapping_tree_detail::device_visitor {
		virtual void visit(btree_path const &, uint64_t) {
		}
	};

	class ll_damage_visitor {
	public:
		ll_damage_visitor(damage_visitor &v)
		: v_(v) {
		}

		virtual void visit(btree_path const &path, btree_detail::damage const &d) {
			switch (path.size()) {
			case 0:
				v_.visit(missing_devices(d.desc_, d.lost_keys_));
				break;

			case 1:
				v_.visit(missing_mappings(d.desc_, path[0], d.lost_keys_));
				break;

			default:
				// shouldn't get here.
				throw std::runtime_error("ll_damage_visitor: path too long");
			}
		}

	private:
		damage_visitor &v_;
	};
}

void
thin_provisioning::walk_mapping_tree(dev_tree const &tree,
				     mapping_tree_detail::device_visitor &dev_v,
				     mapping_tree_detail::damage_visitor &dv)
{
	ll_damage_visitor ll_dv(dv);
	btree_visit_values(tree, dev_v, ll_dv);
}

void
thin_provisioning::check_mapping_tree(dev_tree const &tree,
				      mapping_tree_detail::damage_visitor &visitor)
{
	noop_block_visitor dev_v;
	walk_mapping_tree(tree, dev_v, visitor);
}

void
thin_provisioning::walk_mapping_tree(mapping_tree const &tree,
				     mapping_tree_detail::mapping_visitor &mv,
				     mapping_tree_detail::damage_visitor &dv)
{
	ll_damage_visitor ll_dv(dv);
	btree_visit_values(tree, mv, ll_dv);
}

void
thin_provisioning::check_mapping_tree(mapping_tree const &tree,
				      mapping_tree_detail::damage_visitor &visitor)
{
	noop_block_time_visitor mv;
	walk_mapping_tree(tree, mv, visitor);
}

void
thin_provisioning::walk_mapping_tree(single_mapping_tree const &tree,
				     mapping_tree_detail::mapping_visitor &mv,
				     mapping_tree_detail::damage_visitor &dv)
{
	ll_damage_visitor ll_dv(dv);
	btree_visit_values(tree, mv, ll_dv);
}

void
thin_provisioning::check_mapping_tree(single_mapping_tree const &tree,
				      mapping_tree_detail::damage_visitor &visitor)
{
	noop_block_time_visitor mv;
	walk_mapping_tree(tree, mv, visitor);
}

//----------------------------------------------------------------

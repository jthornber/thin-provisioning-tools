#include "era/bloom_tree.h"
#include "persistent-data/data-structures/btree_damage_visitor.h"
#include "persistent-data/data-structures/bitset.h"

using namespace boost;
using namespace era;
using namespace bloom_tree_detail;
using namespace persistent_data;
using namespace std;

//----------------------------------------------------------------

missing_eras::missing_eras(string const &desc,
			   run<uint32_t> const &eras)
	: damage(desc),
	  eras_(eras)
{
}

void
missing_eras::visit(damage_visitor &v) const {
	v.visit(*this);
}

damaged_bloom_filter::damaged_bloom_filter(string const &desc,
					   uint32_t era,
					   run<uint32_t> missing_bits)
	: damage(desc),
	  era_(era),
	  missing_bits_(missing_bits)
{
}

void
damaged_bloom_filter::visit(damage_visitor &v) const
{
	v.visit(*this);
}

//----------------------------------------------------------------

namespace {
	class ll_bloom_visitor : public bitset_detail::bitset_visitor {
	public:
		typedef persistent_data::transaction_manager::ptr tm_ptr;

		ll_bloom_visitor(tm_ptr tm,
				 bloom_tree_detail::bloom_visitor &bloom_v,
				 bloom_tree_detail::damage_visitor &dv)
			: tm_(tm),
			  bloom_v_(bloom_v),
			  dv_(dv) {
		}

		void visit(btree_path const &path, era_detail const &era) {
			era_ = path[0];
			bitset bs(tm_, era.bloom_root, era.nr_bits);
			bs.walk_bitset(*this);
		}

		void visit(uint32_t index, bool value) {
			bloom_v_.visit(index, value);
		}

		void visit(bitset_detail::missing_bits const &d) {
			dv_.visit(bloom_tree_detail::damaged_bloom_filter("missing bits", era_, d.keys_));
		}

	private:
		tm_ptr tm_;
		uint64_t era_;
		bloom_tree_detail::bloom_visitor &bloom_v_;
		bloom_tree_detail::damage_visitor &dv_;
	};

	class ll_damage_visitor {
	public:
		ll_damage_visitor(damage_visitor &v)
		: v_(v) {
		}

		virtual void visit(btree_path const &path,
				   btree_detail::damage const &d) {
			v_.visit(missing_eras(d.desc_, to_uint32(d.lost_keys_)));
		}

	private:
		template <typename T>
		run<uint32_t> to_uint32(run<T> const &r) {
			return run<uint32_t>(optional<uint32_t>(r.begin_),
					     optional<uint32_t>(r.end_));
		}

		damage_visitor &v_;
	};
}

void
era::walk_bloom_tree(persistent_data::transaction_manager::ptr tm,
		     bloom_tree const &tree,
		     bloom_tree_detail::bloom_visitor &bloom_v,
		     bloom_tree_detail::damage_visitor &dv)
{
	ll_bloom_visitor ll_bv(tm, bloom_v, dv);
	ll_damage_visitor ll_dv(dv);
	btree_visit_values(tree, ll_bv, ll_dv);
}

namespace {
	class noop_bloom_visitor : public bloom_tree_detail::bloom_visitor {
	public:
		void visit(uint32_t index, bool value) {
		}
	};
};

void
era::check_bloom_tree(persistent_data::transaction_manager::ptr tm,
		      bloom_tree const &tree,
		      bloom_tree_detail::damage_visitor &dv)
{
	noop_bloom_visitor bv;
	walk_bloom_tree(tm, tree, bv, dv);
}

//----------------------------------------------------------------

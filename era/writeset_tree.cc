#include "era/writeset_tree.h"
#include "persistent-data/data-structures/btree_damage_visitor.h"
#include "persistent-data/data-structures/bitset.h"

using namespace era;
using namespace writeset_tree_detail;
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

damaged_writeset::damaged_writeset(string const &desc,
				   uint32_t era,
				   run<uint32_t> missing_bits)
	: damage(desc),
	  era_(era),
	  missing_bits_(missing_bits)
{
}

void
damaged_writeset::visit(damage_visitor &v) const
{
	v.visit(*this);
}

//----------------------------------------------------------------

namespace {
	class ll_writeset_visitor : public bitset_detail::bitset_visitor {
	public:
		typedef persistent_data::transaction_manager::ptr tm_ptr;

		ll_writeset_visitor(tm_ptr tm,
				    writeset_tree_detail::writeset_visitor &writeset_v,
				    writeset_tree_detail::damage_visitor &dv)
			: tm_(tm),
			  era_(0),
			  writeset_v_(writeset_v),
			  dv_(dv) {
		}

		void visit(btree_path const &path, era_detail const &era) {
			era_ = path[0];
			persistent_data::bitset bs(*tm_, era.writeset_root, era.nr_bits);
			writeset_v_.writeset_begin(era_, era.nr_bits);
			bs.walk_bitset(*this);
			writeset_v_.writeset_end();
		}

		void visit(uint32_t index, bool value) {
			writeset_v_.bit(index, value);
		}

		void visit(bitset_detail::missing_bits const &d) {
			dv_.visit(writeset_tree_detail::damaged_writeset("missing bits", era_, d.keys_));
		}

	private:
		tm_ptr tm_;
		uint64_t era_;
		writeset_tree_detail::writeset_visitor &writeset_v_;
		writeset_tree_detail::damage_visitor &dv_;
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
			return run<uint32_t>(boost::optional<uint32_t>(r.begin_),
					     boost::optional<uint32_t>(r.end_));
		}

		damage_visitor &v_;
	};
}

void
era::walk_writeset_tree(persistent_data::transaction_manager::ptr tm,
			writeset_tree const &tree,
			writeset_tree_detail::writeset_visitor &writeset_v,
			writeset_tree_detail::damage_visitor &dv)
{
	ll_writeset_visitor ll_bv(tm, writeset_v, dv);
	ll_damage_visitor ll_dv(dv);
	btree_visit_values(tree, ll_bv, ll_dv);
}

namespace {
	class noop_writeset_visitor : public writeset_tree_detail::writeset_visitor {
	public:
		void writeset_begin(uint32_t era, uint32_t nr_bits) {
		}

		void bit(uint32_t index, bool value) {
		}

		void writeset_end() {
		}
	};
};

void
era::check_writeset_tree(persistent_data::transaction_manager::ptr tm,
			 writeset_tree const &tree,
			 writeset_tree_detail::damage_visitor &dv)
{
	noop_writeset_visitor bv;
	walk_writeset_tree(tm, tree, bv, dv);
}

//----------------------------------------------------------------

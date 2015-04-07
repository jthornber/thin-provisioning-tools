#include "era/era_array.h"

using namespace era;
using namespace era_array_detail;
using namespace std;

//----------------------------------------------------------------

missing_eras::missing_eras(string const &desc, run<uint32_t> const &eras)
	: damage(desc),
	  eras_(eras)
{
}

void
missing_eras::visit(damage_visitor &v) const
{
	v.visit(*this);
}

invalid_era::invalid_era(string const &desc, block_address block, uint32_t era)
	: damage(desc),
	  block_(block),
	  era_(era)
{
}

void
invalid_era::visit(damage_visitor &v) const
{
	v.visit(*this);
}

//----------------------------------------------------------------

namespace {
	class check_era_visitor : public era_array_visitor {
	public:
		check_era_visitor(damage_visitor &visitor, uint32_t current_era)
			: visitor_(visitor),
			  current_era_(current_era) {
		}

		virtual void visit(uint32_t cblock, uint32_t era) {
			if (era > current_era_)
				visitor_.visit(invalid_era("era too great", cblock, era));
		}

	private:
		damage_visitor &visitor_;
		uint32_t current_era_;
	};

	class ll_damage_visitor {
	public:
		ll_damage_visitor(damage_visitor &v)
		: v_(v) {
		}

		virtual void visit(array_detail::damage const &d) {
			v_.visit(missing_eras(d.desc_, d.lost_keys_));
		}

	private:
		damage_visitor &v_;
	};
}

void
era::walk_era_array(era_array const &array,
		    era_array_visitor &ev,
		    era_array_detail::damage_visitor &dv)
{
	ll_damage_visitor ll(dv);
	array.visit_values(ev, ll);
}

void
era::check_era_array(era_array const &array,
		     uint32_t current_era,
		     era_array_detail::damage_visitor &dv)
{
	check_era_visitor cv(dv, current_era);
	walk_era_array(array, cv, dv);
}

//----------------------------------------------------------------

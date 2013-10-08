#include "caching/mapping_array.h"
#include "persistent-data/endian_utils.h"

#include <set>

using namespace caching;
using namespace caching::mapping_array_damage;
using namespace std;

//----------------------------------------------------------------

namespace {
	const uint64_t FLAGS_MASK = (1 << 16) - 1;
}

void
mapping_traits::unpack(disk_type const &disk, value_type &value)
{
	uint64_t v = base::to_cpu<uint64_t>(disk);
	value.oblock_ = v >> 16;
	value.flags_ = v & FLAGS_MASK;
}

void
mapping_traits::pack(value_type const &value, disk_type &disk)
{
	uint64_t packed = value.oblock_ << 16;
	packed = packed | (value.flags_ & FLAGS_MASK);
	disk = base::to_disk<le64>(packed);
}

//----------------------------------------------------------------

missing_mappings::missing_mappings(std::string const &desc, run<uint32_t> const &keys)
	: damage(desc),
	  keys_(keys)
{
}

void
missing_mappings::visit(damage_visitor &v) const
{
	v.visit(*this);
}

invalid_mapping::invalid_mapping(std::string const &desc,
				 block_address cblock, mapping const &m)
	: damage(desc),
	  cblock_(cblock),
	  m_(m)
{
}

void
invalid_mapping::visit(damage_visitor &v) const
{
	v.visit(*this);
}

namespace {
	class mapping_visitor {
	public:
		mapping_visitor(damage_visitor &visitor)
		: visitor_(visitor) {
		}

		virtual void visit(uint32_t index, mapping const &m) {
			block_address cblock = index;

			if (!valid_mapping(m))
				return;

			if (seen_oblock(m))
				visitor_.visit(invalid_mapping("origin block already mapped", cblock, m));
			else
				record_oblock(m);

			if (unknown_flags(m))
				visitor_.visit(invalid_mapping("unknown flags in mapping", cblock, m));
		}

	private:
		static bool valid_mapping(mapping const &m) {
			return !!(m.flags_ & M_VALID);
		}

		bool seen_oblock(mapping const &m) const {
			return seen_oblocks_.find(m.oblock_) != seen_oblocks_.end();
		}

		void record_oblock(mapping const &m) {
			seen_oblocks_.insert(m.oblock_);
		}

		static bool unknown_flags(mapping const &m) {
			return (m.flags_ & ~(M_VALID | M_DIRTY));
		}

		damage_visitor &visitor_;
		set<block_address> seen_oblocks_;
	};

	class ll_damage_visitor {
	public:
		ll_damage_visitor(damage_visitor &v)
		: v_(v) {
		}

		virtual void visit(array_detail::damage const &d) {
			v_.visit(missing_mappings(d.desc_, d.lost_keys_));
		}

	private:
		damage_visitor &v_;
	};
}

void
caching::check_mapping_array(mapping_array const &array, damage_visitor &visitor)
{
	mapping_visitor mv(visitor);
	ll_damage_visitor ll(visitor);
	array.visit_values(mv, ll);
}

//----------------------------------------------------------------

#ifndef CACHE_MAPPING_ARRAY_H
#define CACHE_MAPPING_ARRAY_H

#include "persistent-data/data-structures/array.h"

//----------------------------------------------------------------

namespace caching {
	enum mapping_flags {
		M_VALID = 1,
		M_DIRTY = 2
	};

	struct mapping {
		uint64_t oblock_;
		uint32_t flags_;
	};

	struct mapping_traits {
		typedef base::le64 disk_type;
		typedef mapping value_type;
		typedef no_op_ref_counter<value_type> ref_counter;

		static void unpack(disk_type const &disk, value_type &value);
		static void pack(value_type const &value, disk_type &disk);
	};

	namespace mapping_array_damage {
		class damage_visitor;

		struct damage {
			virtual ~damage() {}
			virtual void visit(damage_visitor &v) const = 0;
		};

		struct missing_mappings : public damage {
			missing_mappings(run<uint32_t> const &keys, std::string const &desc);
			virtual void visit(damage_visitor &v) const;

			run<uint32_t> keys_;
			std::string desc_;
		};

		class damage_visitor {
		public:
			virtual ~damage_visitor() {}

			virtual void visit(mapping_array_damage::damage const &d) {
				d.visit(*this);
			}

			virtual void visit(missing_mappings const &d) = 0;
		};
	}

	typedef persistent_data::array<mapping_traits> mapping_array;

	void check_mapping_array(mapping_array const &array,
				 mapping_array_damage::damage_visitor &visitor);
}

//----------------------------------------------------------------

#endif

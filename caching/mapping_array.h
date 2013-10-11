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

		class damage {
		public:
			damage(std::string const &desc)
				: desc_(desc) {
			}

			virtual ~damage() {}
			virtual void visit(damage_visitor &v) const = 0;

			std::string get_desc() const {
				return desc_;
			}

		private:
			std::string desc_;
		};

		struct missing_mappings : public damage {
			missing_mappings(std::string const &desc, run<uint32_t> const &keys);
			virtual void visit(damage_visitor &v) const;

			run<uint32_t> keys_;
		};

		struct invalid_mapping : public damage {
			invalid_mapping(std::string const &desc, block_address cblock, mapping const &m);
			virtual void visit(damage_visitor &v) const;

			block_address cblock_;
			mapping m_;
		};

		class damage_visitor {
		public:
			virtual ~damage_visitor() {}

			void visit(mapping_array_damage::damage const &d) {
				d.visit(*this);
			}

			virtual void visit(missing_mappings const &d) = 0;
			virtual void visit(invalid_mapping const &d) = 0;
		};
	}

	typedef persistent_data::array<mapping_traits> mapping_array;

	class mapping_visitor {
	public:
		virtual ~mapping_visitor() {}

		void visit(uint32_t index, mapping const &m) {
			visit(static_cast<block_address>(index), m);
		}

		virtual void visit(block_address cblock, mapping const &m) = 0;
	};

	void walk_mapping_array(mapping_array const &array,
				mapping_visitor &mv,
				mapping_array_damage::damage_visitor &dv);

	void check_mapping_array(mapping_array const &array,
				 mapping_array_damage::damage_visitor &visitor);
}

//----------------------------------------------------------------

#endif

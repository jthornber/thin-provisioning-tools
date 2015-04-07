#ifndef ERA_ARRAY_H
#define ERA_ARRAY_H

#include "persistent-data/data-structures/array.h"
#include "persistent-data/data-structures/simple_traits.h"

//----------------------------------------------------------------

namespace era {
	namespace era_array_detail {
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

		struct missing_eras : public damage {
			missing_eras(std::string const &desc, run<uint32_t> const &eras);
			virtual void visit(damage_visitor &v) const;

			run<uint32_t> eras_;
		};

		struct invalid_era : public damage {
			invalid_era(std::string const &desc, block_address block, uint32_t era);
			virtual void visit(damage_visitor &v) const;

			block_address block_;
			uint32_t era_;
		};

		class damage_visitor {
		public:
			virtual ~damage_visitor() {}

			void visit(era_array_detail::damage const &d) {
				d.visit(*this);
			}

			virtual void visit(missing_eras const &d) = 0;
			virtual void visit(invalid_era const &d) = 0;
		};
	}

	typedef persistent_data::array<uint32_traits> era_array;

	class era_array_visitor {
	public:
		virtual ~era_array_visitor() {}

		virtual void visit(uint32_t index, uint32_t era) = 0;
	};

	void walk_era_array(era_array const &array,
			    era_array_visitor &ev,
			    era_array_detail::damage_visitor &dv);

	void check_era_array(era_array const &array,
			     uint32_t current_era,
			     era_array_detail::damage_visitor &dv);
}

//----------------------------------------------------------------

#endif

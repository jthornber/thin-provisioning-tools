#ifndef ERA_WRITESET_TREE_H
#define ERA_WRITESET_TREE_H

#include "era/era_detail.h"
#include "persistent-data/data-structures/btree.h"

//----------------------------------------------------------------

namespace era {
	namespace writeset_tree_detail {
		class damage_visitor;

		class damage {
		public:
			damage(std::string const &desc)
				: desc_(desc) {
			}

			virtual ~damage() {}
			virtual void visit(damage_visitor &v) const = 0;

			std::string const &get_desc() const {
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

		struct damaged_writeset : public damage {
			damaged_writeset(std::string const &desc,
					 uint32_t era,
					 run<uint32_t> missing_bits);
			virtual void visit(damage_visitor &v) const;

			uint32_t era_;
			run<uint32_t> missing_bits_;
		};

		class damage_visitor {
		public:
			typedef boost::shared_ptr<damage_visitor> ptr;

			virtual ~damage_visitor() {}

			void visit(damage const &d) {
				d.visit(*this);
			}

			virtual void visit(missing_eras const &d) = 0;
			virtual void visit(damaged_writeset const &d) = 0;
		};

		class writeset_visitor {
		public:
			typedef boost::shared_ptr<writeset_visitor> ptr;

			virtual ~writeset_visitor() {}

			virtual void writeset_begin(uint32_t era, uint32_t nr_bits) = 0;
			virtual void bit(uint32_t index, bool value) = 0;
			virtual void writeset_end() = 0;
		};
	}

	typedef persistent_data::btree<1, era_detail_traits> writeset_tree;

	void walk_writeset_tree(persistent_data::transaction_manager::ptr tm,
				writeset_tree const &tree,
				writeset_tree_detail::writeset_visitor &writeset_v,
				writeset_tree_detail::damage_visitor &dv);

	void check_writeset_tree(persistent_data::transaction_manager::ptr tm,
				 writeset_tree const &tree,
				 writeset_tree_detail::damage_visitor &dv);
}

//----------------------------------------------------------------

#endif

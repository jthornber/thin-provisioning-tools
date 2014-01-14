#ifndef ERA_BLOOM_TREE_H
#define ERA_BLOOM_TREE_H

#include "era/era_detail.h"
#include "persistent-data/data-structures/btree.h"

//----------------------------------------------------------------

namespace era {
	namespace bloom_tree_detail {
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

		struct damaged_bloom_filter : public damage {
			damaged_bloom_filter(std::string const &desc,
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
			virtual void visit(damaged_bloom_filter const &d) = 0;
		};

		class bloom_visitor {
		public:
			typedef boost::shared_ptr<bloom_visitor> ptr;

			virtual ~bloom_visitor() {}
			virtual void visit(uint32_t index, bool value) = 0;
		};
	}

	typedef persistent_data::btree<1, era_detail_traits> bloom_tree;

	void walk_bloom_tree(persistent_data::transaction_manager::ptr tm,
			     bloom_tree const &tree,
			     bloom_tree_detail::bloom_visitor &bloom_v,
			     bloom_tree_detail::damage_visitor &dv);

	void check_bloom_tree(persistent_data::transaction_manager::ptr tm,
			      bloom_tree const &tree,
			      bloom_tree_detail::damage_visitor &dv);
}

//----------------------------------------------------------------

#endif

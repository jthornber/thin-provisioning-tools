#ifndef PERSISTENT_DATA_DATA_STRUCTURES_BTREE_COUNTER_H
#define PERSISTENT_DATA_DATA_STRUCTURES_BTREE_COUNTER_H

#include "persistent-data/data-structures/btree.h"
#include "persistent-data/data-structures/btree_base_visitor.h"
#include "persistent-data/data-structures/btree_damage_visitor.h"
#include "persistent-data/block_counter.h"

//----------------------------------------------------------------

namespace persistent_data {
	namespace btree_count_detail {
		template <typename ValueVisitor, typename DamageVisitor, unsigned Levels, typename ValueTraits, typename ValueCounter>
		class counting_visitor : public btree_damage_visitor<ValueVisitor, DamageVisitor, Levels, ValueTraits> {
			typedef btree_damage_visitor<ValueVisitor, DamageVisitor, Levels, ValueTraits> BtreeDamageVisitor;
		public:
			typedef btree<Levels, ValueTraits> tree;

			counting_visitor(ValueVisitor &value_visitor,
					 DamageVisitor &damage_visitor,
					 block_counter &bc,
					 ValueCounter &vc)
				: BtreeDamageVisitor(value_visitor, damage_visitor, false),
				  bc_(bc),
				  vc_(vc) {
			}

			virtual bool visit_internal(node_location const &l,
						    typename tree::internal_node const &n) {
				return BtreeDamageVisitor::visit_internal(l, n) ?
					visit_node(n) : false;
			}

			virtual bool visit_internal_leaf(node_location const &l,
							 typename tree::internal_node const &n) {
				return BtreeDamageVisitor::visit_internal_leaf(l, n) ?
					visit_node(n) : false;
			}

			virtual bool visit_leaf(node_location const &l,
						typename tree::leaf_node const &n) {
				if (BtreeDamageVisitor::visit_leaf(l, n) && visit_node(n)) {
					unsigned nr = n.get_nr_entries();

					for (unsigned i = 0; i < nr; i++) {
						// FIXME: confirm l2 is correct
						node_location l2(l);
						l2.push_key(i);
						vc_.visit(l2, n.value_at(i));
					}

					return true;
				}

				return false;
			}

		private:
			template <typename Node>
			bool visit_node(Node const &n) {
				block_address b = n.get_location();
				bool seen = bc_.get_count(b);
				bc_.inc(b);
				return !seen;
			}

			block_counter &bc_;
			ValueCounter &vc_;
		};
	}

	template <typename T>
	struct noop_value_counter {
		void visit(btree_detail::node_location const &loc, T const &v) {
		}
	};

	struct block_address_counter {
		block_address_counter(block_counter &bc)
		: bc_(bc) {
		}

		void visit(btree_detail::node_location const &loc, block_address b) {
			bc_.inc(b);
		}

	private:
		block_counter &bc_;
	};

	// Counts how many times each metadata block is referenced in the
	// tree.  Blocks already referenced in the block counter are not
	// walked.  This walk should only be done once you're sure the tree
	// is not corrupt.
	template <unsigned Levels, typename ValueTraits, typename ValueCounter>
	void count_btree_blocks(btree<Levels, ValueTraits> const &tree, block_counter &bc, ValueCounter &vc) {
		typedef noop_value_visitor<typename ValueTraits::value_type> NoopValueVisitor;
		NoopValueVisitor noop_vv;
		noop_damage_visitor noop_dv;
		btree_count_detail::counting_visitor<NoopValueVisitor, noop_damage_visitor, Levels, ValueTraits, ValueCounter> v(noop_vv, noop_dv, bc, vc);
		tree.visit_depth_first(v);
	}

	template <unsigned Levels, typename ValueTraits>
	void count_btree_blocks(btree<Levels, ValueTraits> const &tree, block_counter &bc) {
		typedef noop_value_visitor<typename ValueTraits::value_type> NoopValueVisitor;
		NoopValueVisitor noop_vv;
		noop_damage_visitor noop_dv;
		typedef noop_value_counter<typename ValueTraits::value_type> NoopValueCounter;
		NoopValueCounter vc;
		btree_count_detail::counting_visitor<NoopValueVisitor, noop_damage_visitor, Levels, ValueTraits, NoopValueCounter> v(noop_vv, noop_dv, bc, vc);
		tree.visit_depth_first(v);
	}
}

//----------------------------------------------------------------

#endif

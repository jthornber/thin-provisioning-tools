#ifndef PERSISTENT_DATA_DATA_STRUCTURES_BTREE_COUNTER_H
#define PERSISTENT_DATA_DATA_STRUCTURES_BTREE_COUNTER_H

#include "persistent-data/data-structures/btree.h"
#include "persistent-data/block_counter.h"

//----------------------------------------------------------------

namespace persistent_data {
	namespace btree_count_detail {
		template <unsigned Levels, typename ValueTraits, typename ValueCounter>
		class counting_visitor : public btree<Levels, ValueTraits>::visitor {
		public:
			typedef btree<Levels, ValueTraits> tree;

			counting_visitor(block_counter &bc, ValueCounter &vc)
				: bc_(bc),
				  vc_(vc) {
			}

			virtual bool visit_internal(node_location const &l,
						    typename tree::internal_node const &n) {
				return visit_node(n);
			}

			virtual bool visit_internal_leaf(node_location const &l,
							 typename tree::internal_node const &n) {
				return visit_node(n);
			}

			virtual bool visit_leaf(node_location const &l,
						typename tree::leaf_node const &n) {
				if (visit_node(n)) {
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
		btree_count_detail::counting_visitor<Levels, ValueTraits, ValueCounter> v(bc, vc);
		tree.visit_depth_first(v);
	}
}

//----------------------------------------------------------------

#endif

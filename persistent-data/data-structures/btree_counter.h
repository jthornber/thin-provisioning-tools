#ifndef PERSISTENT_DATA_DATA_STRUCTURES_BTREE_COUNTER_H
#define PERSISTENT_DATA_DATA_STRUCTURES_BTREE_COUNTER_H

#include "persistent-data/data-structures/btree.h"
#include "persistent-data/data-structures/btree_node_checker.h"
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
				return check_internal(l, n) ? visit_node(n) : false;
			}

			virtual bool visit_internal_leaf(node_location const &l,
							 typename tree::internal_node const &n) {
				return check_leaf(l, n) ? visit_node(n) : false;
			}

			virtual bool visit_leaf(node_location const &l,
						typename tree::leaf_node const &n) {
				if (check_leaf(l, n) && visit_node(n)) {
					unsigned nr = n.get_nr_entries();

					for (unsigned i = 0; i < nr; i++) {
						// FIXME: confirm l2 is correct
						node_location l2(l);
						l2.push_key(n.key_at(i));
						vc_.visit(l2, n.value_at(i));
					}

					return true;
				}

				return false;
			}

			typedef typename btree<Levels, ValueTraits>::visitor::error_outcome error_outcome;

			error_outcome error_accessing_node(node_location const &l, block_address b,
							   std::string const &what) {
				return btree<Levels, ValueTraits>::visitor::EXCEPTION_HANDLED;
			}

		private:
			bool check_internal(node_location const &l,
					    btree_detail::node_ref<block_traits> const &n) {
				if (l.is_sub_root())
					new_root(l.level());

				if (!checker_.check_block_nr(n) ||
				    !checker_.check_value_size(n) ||
				    !checker_.check_max_entries(n) ||
				    !checker_.check_nr_entries(n, l.is_sub_root()) ||
				    !checker_.check_ordered_keys(n) ||
				    !checker_.check_parent_key(n, l.is_sub_root() ? boost::optional<uint64_t>() : l.key))
					return false;

				return true;
			}

			template <typename ValueTraits2>
			bool check_leaf(node_location const &l,
				        btree_detail::node_ref<ValueTraits2> const &n) {
				if (l.is_sub_root())
					new_root(l.level());

				if (!checker_.check_block_nr(n) ||
				    !checker_.check_value_size(n) ||
				    !checker_.check_max_entries(n) ||
				    !checker_.check_nr_entries(n, l.is_sub_root()) ||
				    !checker_.check_ordered_keys(n) ||
				    !checker_.check_parent_key(n, l.is_sub_root() ? boost::optional<uint64_t>() : l.key) ||
				    !checker_.check_leaf_key(n, last_leaf_key_[l.level()]))
					return false;

				if (n.get_nr_entries() > 0)
					last_leaf_key_[l.level()] = n.key_at(n.get_nr_entries() - 1);

				return true;
			}

			void new_root(unsigned level) {
				// we're starting a new subtree, so should
				// reset the last_leaf value.
				last_leaf_key_[level] = boost::optional<uint64_t>();
			}

			template <typename Node>
			bool visit_node(Node const &n) {
				block_address b = n.get_location();
				bool seen = bc_.get_count(b);
				bc_.inc(b);
				return !seen;
			}

			block_counter &bc_;
			ValueCounter &vc_;
			btree_node_checker checker_;
			boost::optional<uint64_t> last_leaf_key_[Levels];
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

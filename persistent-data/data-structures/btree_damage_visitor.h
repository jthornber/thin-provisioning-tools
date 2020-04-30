#ifndef PERSISTENT_DATA_DATA_STRUCTURES_DAMAGE_VISITOR_H
#define PERSISTENT_DATA_DATA_STRUCTURES_DAMAGE_VISITOR_H

#include "persistent-data/data-structures/btree.h"
#include "persistent-data/data-structures/btree_node_checker.h"
#include "persistent-data/run.h"

//----------------------------------------------------------------

namespace persistent_data {
	namespace btree_detail {
		struct damage {
			typedef std::shared_ptr<damage> ptr;

			damage(run<uint64_t> lost_keys,
			       std::string const &desc)
				: lost_keys_(lost_keys),
				  desc_(desc) {
			}

			run<uint64_t> lost_keys_;
			std::string desc_;
		};

		inline std::ostream &operator <<(std::ostream &out, damage const &d) {
			out << "btree damage[lost_keys = " << d.lost_keys_
			    << ", \"" << d.desc_ << "\"]";
			return out;
		}

		class noop_damage_visitor {
		public:
			virtual void visit(btree_path const &path, damage const &d) {
			}
		};

		// Tracks damage in a single level btree.  Use multiple
		// trackers if you have a multilayer tree.
		class damage_tracker {
		public:
			damage_tracker();

			typedef run<uint64_t> run64;
			typedef boost::optional<run64> maybe_run64;

			void bad_node();

			maybe_run64 good_internal(block_address begin);

			// remember 'end' is the one-past-the-end value, so
			// take the last key in the leaf and add one.
			maybe_run64 good_leaf(block_address begin, block_address end);

			maybe_run64 end();

		private:
			bool damaged_;
			block_address damage_begin_;
		};

		// As we walk a btree we need to know if we've moved into a
		// different sub tree (by looking at the btree_path).
		class path_tracker {
		public:
			path_tracker();

			// returns the old path if the tree has changed.
			btree_path const *next_path(btree_path const &p);

			btree_path const &current_path() const;

		private:
			std::list<btree_path> paths_;
		};

		//----------------------------------------------------------------

		// This class implements consistency checking for the btrees.  It
		// also allows the caller to visit all accessible values.

		// Derive from this if you want some additional checks.  It's worth
		// summarising what is checked:

		//
		// Implemented
		// -----------
		//
		// - block_nr
		// - nr_entries < max_entries
		// - max_entries fits in block
		// - max_entries is divisible by 3
		// - nr_entries > minimum (except for root nodes)
		//
		// Not implemented
		// ---------------
		//
		// - leaf | internal flags (this can be inferred from siblings)

		//----------------------------------------------------------------

		template <typename ValueVisitor, typename DamageVisitor, uint32_t Levels, typename ValueTraits>
		class btree_damage_visitor : public btree<Levels, ValueTraits>::visitor {
		public:
			typedef btree_detail::node_location node_location;
			typedef run<block_address> run64;
			typedef boost::optional<run64> maybe_run64;

			btree_damage_visitor(ValueVisitor &value_visitor,
					     DamageVisitor &damage_visitor)
				: avoid_repeated_visits_(true),
				  value_visitor_(value_visitor),
				  damage_visitor_(damage_visitor) {
			}

			bool visit_internal(node_location const &loc,
					    btree_detail::node_ref<block_traits> const &n) {
				update_path(loc.path);

				return check_internal(loc, n);
			}

			bool visit_internal_leaf(node_location const &loc,
						 btree_detail::node_ref<block_traits> const &n) {
				update_path(loc.path);

				return check_leaf(loc, n);
			}

			bool visit_leaf(node_location const &loc,
					btree_detail::node_ref<ValueTraits> const &n) {
				update_path(loc.path);


				bool r = check_leaf(loc, n);

				// If anything goes wrong with the checks, we skip
				// the value visiting.
				if (!r)
					return false;

				visit_values(loc.path, n);

				return true;
			}

			void visit_complete() {
				end_walk();
			}

			typedef typename btree<Levels, ValueTraits>::visitor::error_outcome error_outcome;

			error_outcome error_accessing_node(node_location const &l, block_address b,
							   std::string const &what) {
				update_path(l.path);
				report_damage(what);
				return btree<Levels, ValueTraits>::visitor::EXCEPTION_HANDLED;
			}

		private:
			void visit_values(btree_path const &path,
					  node_ref<ValueTraits> const &n) {
				btree_path p2(path);
				unsigned nr = n.get_nr_entries();
				for (unsigned i = 0; i < nr; i++) {
					p2.push_back(n.key_at(i));
					value_visitor_.visit(p2, n.value_at(i));
					p2.pop_back();
				}
			}

			bool check_internal(node_location const &loc,
					    btree_detail::node_ref<block_traits> const &n) {
				if (loc.is_sub_root())
					new_root(loc.level());

				if (already_visited(n))
					return false;
				else if (!checker_.check_block_nr(n) ||
					 !checker_.check_value_size(n) ||
					 !checker_.check_max_entries(n) ||
					 !checker_.check_nr_entries(n, loc.is_sub_root()) ||
					 !checker_.check_ordered_keys(n) ||
					 !checker_.check_parent_key(n, loc.is_sub_root() ? boost::optional<uint64_t>() : loc.key)) {
					report_damage(checker_.get_last_error_string());

					return false;
				}

				good_internal(n.key_at(0));

				return true;
			}

			template <typename ValueTraits2>
			bool check_leaf(node_location const &loc,
					btree_detail::node_ref<ValueTraits2> const &n) {
				if (loc.is_sub_root())
					new_root(loc.level());

				if (already_visited(n))
					return false;
				else if (!checker_.check_block_nr(n) ||
					 !checker_.check_value_size(n) ||
					 !checker_.check_max_entries(n) ||
					 !checker_.check_nr_entries(n, loc.is_sub_root()) ||
					 !checker_.check_ordered_keys(n) ||
					 !checker_.check_parent_key(n, loc.is_sub_root() ? boost::optional<uint64_t>() : loc.key) ||
					 !checker_.check_leaf_key(n, last_leaf_key_[loc.level()])) {
					report_damage(checker_.get_last_error_string());

					return false;
				}

				if (n.get_nr_entries() > 0) {
					last_leaf_key_[loc.level()] = n.key_at(n.get_nr_entries() - 1);
					good_leaf(n.key_at(0), n.key_at(n.get_nr_entries() - 1) + 1);
				}

				return true;
			}

			template <typename node>
			bool already_visited(node const &n) {
				block_address b = n.get_location();

				if (avoid_repeated_visits_) {
					if (seen_.count(b) > 0)
						return true;

					seen_.insert(b);
				}

				return false;
			}

			void new_root(unsigned level) {
				// we're starting a new subtree, so should
				// reset the last_leaf value.
				last_leaf_key_[level] = boost::optional<uint64_t>();
			}

			//--------------------------------

			// damage tracking

			void report_damage(std::string const &desc) {
				damage_reasons_.push_back(desc);
				dt_.bad_node();
			}

			void good_internal(block_address b) {
				maybe_run64 mr = dt_.good_internal(b);
				if (mr)
					issue_damage(path_tracker_.current_path(), *mr);
			}

			void good_leaf(block_address b, block_address e) {
				maybe_run64 mr = dt_.good_leaf(b, e);

				if (mr)
					issue_damage(path_tracker_.current_path(), *mr);
			}

			void end_walk() {
				maybe_issue_damage(path_tracker_.current_path());
			}

			void issue_damage(btree_path const &path, run64 const &r) {
				damage d(r, build_damage_desc());
				clear_damage_desc();
				damage_visitor_.visit(path, d);
			}

			std::string build_damage_desc() const {
				std::string r;

				std::list<std::string>::const_iterator it, end = damage_reasons_.end();
				for (it = damage_reasons_.begin(); it != end; ++it)
					r += *it;

				return r;
			}

			void clear_damage_desc() {
				damage_reasons_.clear();
			}

			void maybe_issue_damage(btree_path const &path) {
				maybe_run64 mr = dt_.end();
				if (mr)
					issue_damage(path, *mr);
			}

			void update_path(btree_path const &path) {
				btree_path const *old_path = path_tracker_.next_path(path);
				if (old_path)
					// we need to emit any errors that
					// were accrued against the old
					// path.
					maybe_issue_damage(*old_path);
			}

			//--------------------------------

			bool avoid_repeated_visits_;

			ValueVisitor &value_visitor_;
			DamageVisitor &damage_visitor_;

			std::set<block_address> seen_;
			boost::optional<uint64_t> last_leaf_key_[Levels];

			btree_node_checker checker_;
			path_tracker path_tracker_;
			damage_tracker dt_;
			std::list<std::string> damage_reasons_;
		};
	}

	template <unsigned Levels, typename ValueTraits, typename ValueVisitor, typename DamageVisitor>
	void btree_visit_values(btree<Levels, ValueTraits> const &tree,
				ValueVisitor &value_visitor,
				DamageVisitor &damage_visitor) {
		btree_detail::btree_damage_visitor<ValueVisitor, DamageVisitor, Levels, ValueTraits>
			v(value_visitor, damage_visitor);
		tree.visit_depth_first(v);
	}
}

//----------------------------------------------------------------

#endif

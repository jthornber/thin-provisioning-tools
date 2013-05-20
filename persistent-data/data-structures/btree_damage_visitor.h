#ifndef PERSISTENT_DATA_DATA_STRUCTURES_DAMAGE_VISITOR_H
#define PERSISTENT_DATA_DATA_STRUCTURES_DAMAGE_VISITOR_H

#include "persistent-data/data-structures/btree.h"
#include "persistent-data/range.h"

//----------------------------------------------------------------

namespace persistent_data {

	namespace btree_detail {
		struct damage {
			typedef boost::shared_ptr<damage> ptr;

			damage(range<uint64_t> lost_keys,
			       std::string const &desc)
				: lost_keys_(lost_keys),
				  desc_(desc) {
			}

			range<uint64_t> lost_keys_;
			std::string desc_;
		};

		inline std::ostream &operator <<(std::ostream &out, damage const &d) {
			out << "btree damage[lost_keys = " << d.lost_keys_
			    << ", \"" << d.desc_ << "\"]";
			return out;
		}

		// Tracks damage in a single level btree.  Use multiple
		// trackers if you have a multilayer tree.
		class damage_tracker {
		public:
			damage_tracker()
				: damaged_(false),
				  damage_begin_(0) {
			}

			typedef range<block_address> range64;
			typedef boost::optional<range64> maybe_range64;

			void bad_node() {
				damaged_ = true;
			}

			maybe_range64 good_internal(block_address begin) {
				maybe_range64 r;

				if (damaged_) {
					r = maybe_range64(range64(damage_begin_, begin));
					damaged_ = false;
				}

				damage_begin_ = begin;
				return r;
			}

			// remembe 'end' is the one-past-the-end value, so
			// take the last key in the leaf and add one.
			maybe_range64 good_leaf(block_address begin, block_address end) {
				maybe_range64 r;

				if (damaged_) {
					r = maybe_range64(range64(damage_begin_, begin));
					damaged_ = false;
				}

				damage_begin_ = end;
				return r;
			}

			maybe_range64 end() {
				if (damaged_)
					return maybe_range64(damage_begin_);
				else
					return maybe_range64();
			}

		private:
			bool damaged_;
			block_address damage_begin_;
		};

		// As we walk a btree we need to know if we've moved into a
		// different sub tree (by looking at the btree_path).
		class path_tracker {
		public:
			// returns the old path if the tree has changed.
			boost::optional<btree_path> next_path(btree_path const &p) {
				if (p != path_) {
					btree_path tmp(path_);
					path_ = p;
					return boost::optional<btree_path>(tmp);
				}

				return boost::optional<btree_path>();
			}

			btree_path const &current_path() const {
				return path_;
			}

		private:
			btree_path path_;
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
			typedef range<block_address> range64;
			typedef boost::optional<range64> maybe_range64;

			btree_damage_visitor(block_counter &counter,
					     ValueVisitor &value_visitor,
					     DamageVisitor &damage_visitor)
				: counter_(counter),
				  avoid_repeated_visits_(true),
				  value_visitor_(value_visitor),
				  damage_visitor_(damage_visitor) {
			}

			bool visit_internal(node_location const &loc,
					    btree_detail::node_ref<uint64_traits> const &n) {
				update_path(loc.path);

				return check_internal(loc, n);
			}

			bool visit_internal_leaf(node_location const &loc,
						 btree_detail::node_ref<uint64_traits> const &n) {
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
				report_damage(what);
				return btree<Levels, ValueTraits>::visitor::EXCEPTION_HANDLED;
			}

		private:
			void visit_values(btree_path const &path,
					  node_ref<ValueTraits> const &n) {
				unsigned nr = n.get_nr_entries();
				for (unsigned i = 0; i < nr; i++)
					value_visitor_.visit(path, n.value_at(i));
			}

			bool check_internal(node_location const &loc,
					    btree_detail::node_ref<uint64_traits> const &n) {
				if (!already_visited(n) &&
				    check_block_nr(n) &&
				    check_max_entries(n) &&
				    check_nr_entries(n, loc.is_sub_root()) &&
				    check_ordered_keys(n) &&
				    check_parent_key(loc.is_sub_root() ? optional<uint64_t>() : loc.key, n)) {
					if (loc.is_sub_root())
						new_root(loc.level());

					good_internal(n.key_at(0));
					return true;
				}

				return false;
			}

			template <typename ValueTraits2>
			bool check_leaf(node_location const &loc,
					btree_detail::node_ref<ValueTraits2> const &n) {
				if (!already_visited(n) &&
				    check_block_nr(n) &&
				    check_max_entries(n) &&
				    check_nr_entries(n, loc.is_sub_root()) &&
				    check_ordered_keys(n) &&
				    check_parent_key(loc.is_sub_root() ? optional<uint64_t>() : loc.key, n)) {
					if (loc.is_sub_root())
						new_root(loc.level());

					bool r = check_leaf_key(loc.level(), n);
					if (r && n.get_nr_entries() > 0)
						good_leaf(n.key_at(0), n.key_at(n.get_nr_entries() - 1) + 1);

					return r;
				}

				return false;
			}


			template <typename node>
			bool already_visited(node const &n) {
				block_address b = n.get_location();

				counter_.inc(b);

				if (avoid_repeated_visits_) {
					if (seen_.count(b) > 0)
						return true;

					seen_.insert(b);
				}

				return false;
			}

			template <typename node>
			bool check_block_nr(node const &n) {
				if (n.get_location() != n.get_block_nr()) {
					std::ostringstream out;
					out << "block number mismatch: actually "
					    << n.get_location()
					    << ", claims " << n.get_block_nr();

					report_damage(out.str());
					return false;
				}

				return true;
			}

			template <typename node>
			bool check_max_entries(node const &n) {
				size_t elt_size = sizeof(uint64_t) + n.get_value_size();
				if (elt_size * n.get_max_entries() + sizeof(node_header) > MD_BLOCK_SIZE) {
					std::ostringstream out;
					out << "max entries too large: " << n.get_max_entries();
					report_damage(out.str());
					return false;
				}

				if (n.get_max_entries() % 3) {
					std::ostringstream out;
					out << "max entries is not divisible by 3: " << n.get_max_entries();
					report_damage(out.str());
					return false;
				}

				return true;
			}

			template <typename node>
			bool check_nr_entries(node const &n, bool is_root) {
				if (n.get_nr_entries() > n.get_max_entries()) {
					std::ostringstream out;
					out << "bad nr_entries: "
					    << n.get_nr_entries() << " < "
					    << n.get_max_entries();
					report_damage(out.str());
					return false;
				}

				block_address min = n.get_max_entries() / 3;
				if (!is_root && (n.get_nr_entries() < min)) {
					ostringstream out;
					out << "too few entries in btree_node: "
					    << n.get_nr_entries()
					    << ", expected at least "
					    << min
					    << "(max_entries = " << n.get_max_entries() << ")";
					report_damage(out.str());
					return false;
				}

				return true;
			}

			template <typename node>
			bool check_ordered_keys(node const &n) {
				unsigned nr_entries = n.get_nr_entries();

				if (nr_entries == 0)
					return true; // can only happen if a root node

				uint64_t last_key = n.key_at(0);

				for (unsigned i = 1; i < nr_entries; i++) {
					uint64_t k = n.key_at(i);
					if (k <= last_key) {
						ostringstream out;
						out << "keys are out of order, " << k << " <= " << last_key;
						report_damage(out.str());
						return false;
					}
					last_key = k;
				}

				return true;
			}

			template <typename node>
			bool check_parent_key(boost::optional<uint64_t> key, node const &n) {
				if (!key)
					return true;

				if (*key > n.key_at(0)) {
					ostringstream out;
					out << "parent key mismatch: parent was " << *key
					    << ", but lowest in node was " << n.key_at(0);
					report_damage(out.str());
					return false;
				}

				return true;
			}

			template <typename node>
			bool check_leaf_key(unsigned level, node const &n) {
				if (n.get_nr_entries() == 0)
					return true; // can only happen if a root node

				if (last_leaf_key_[level] && *last_leaf_key_[level] >= n.key_at(0)) {
					ostringstream out;
					out << "the last key of the previous leaf was " << *last_leaf_key_[level]
					    << " and the first key of this leaf is " << n.key_at(0);
					report_damage(out.str());
					return false;
				}

				last_leaf_key_[level] = n.key_at(n.get_nr_entries() - 1);
				return true;
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
				maybe_range64 mr = dt_.good_internal(b);
				if (mr)
					issue_damage(path_tracker_.current_path(), *mr);
			}

			void good_leaf(block_address b, block_address e) {
				maybe_range64 mr = dt_.good_leaf(b, e);

				if (mr)
					issue_damage(path_tracker_.current_path(), *mr);
			}

			// FIXME: duplicate code
			void end_walk() {
				maybe_range64 mr = dt_.end();
				if (mr)
					issue_damage(path_tracker_.current_path(), *mr);
			}

			void issue_damage(btree_path const &path, range64 const &r) {
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

			//--------------------------------

			void update_path(btree_path const &path) {
				boost::optional<btree_path> old_path = path_tracker_.next_path(path);
				if (old_path) {
					// we need to emit any errors that
					// were accrued against the old
					// path.

					// FIXME: duplicate code with end_walk()
					maybe_range64 mr = dt_.end();
					if (mr)
						issue_damage(*old_path, *mr);
				}
			}

			//--------------------------------

			block_counter &counter_;
			bool avoid_repeated_visits_;

			ValueVisitor &value_visitor_;
			DamageVisitor &damage_visitor_;

			std::set<block_address> seen_;
			boost::optional<uint64_t> last_leaf_key_[Levels];

			path_tracker path_tracker_;
			damage_tracker dt_;
			std::list<std::string> damage_reasons_;
		};
	}

	template <unsigned Levels, typename ValueTraits, typename ValueVisitor, typename DamageVisitor>
	void btree_visit_values(btree<Levels, ValueTraits> const &tree,
				block_counter &counter,
				ValueVisitor &value_visitor,
				DamageVisitor &damage_visitor) {
		btree_detail::btree_damage_visitor<ValueVisitor, DamageVisitor, Levels, ValueTraits>
			v(counter, value_visitor, damage_visitor);
		tree.visit_depth_first(v);
	}
}

//----------------------------------------------------------------

#endif

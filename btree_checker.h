#ifndef BTREE_CHECKER_H
#define BTREE_CHECKER_H

#include "btree.h"

#include "checksum.h"
#include "error_set.h"
#include "hex_dump.h"

#include <sstream>
#include <map>
#include <set>

using namespace persistent_data;
using namespace std;

//----------------------------------------------------------------

namespace persistent_data {
	//----------------------------------------------------------------
	// Little helper class that keeps track of how many times blocks
	// are referenced.
	//----------------------------------------------------------------
	class block_counter {
	public:
		typedef std::map<block_address, unsigned> count_map;

		void inc(block_address b) {
			count_map::iterator it = counts_.find(b);
			if (it == counts_.end())
				counts_.insert(make_pair(b, 1));
			else
				it->second++;
		}

		unsigned get_count(block_address b) const {
			count_map::const_iterator it = counts_.find(b);
			return (it == counts_.end()) ? 0 : it->second;
		}

		count_map const &get_counts() const {
			return counts_;
		}

	private:
		count_map counts_;
	};

	//----------------------------------------------------------------
	// This class implements consistency checking for the btrees in
	// general.  Derive from this if you want some additional checks.
	// It's worth summarising what is checked:
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
	// - checksum
	// - leaf | internal flags (this can be inferred from siblings)
	//----------------------------------------------------------------
	template <uint32_t Levels, typename ValueTraits>
	class btree_checker : public btree<Levels, ValueTraits>::visitor {
	public:
		btree_checker(block_counter &counter)
			: counter_(counter),
			  errs_(new error_set("btree errors")) {
		}

		bool visit_internal(unsigned level,
				    optional<uint64_t> key,
				    btree_detail::node_ref<uint64_traits> const &n) {
			if (already_visited(n))
				return false;

			check_sum(n);

			if (!key)
			  new_root(level);

			check_block_nr(n);
			check_max_entries(n);
			check_nr_entries(n, !key);
			check_ordered_keys(n);
			check_parent_key(key, n);
			return true;
		}

		bool visit_internal_leaf(unsigned level,
					 optional<uint64_t> key,
					 btree_detail::node_ref<uint64_traits> const &n) {
			if (already_visited(n))
				return false;

			check_sum(n);

			if (!key)
			  new_root(level);

			check_block_nr(n);
			check_max_entries(n);
			check_nr_entries(n, !key);
			check_ordered_keys(n);
			check_parent_key(key, n);
			check_leaf_key(level, n);

			return true;
		}

		bool visit_leaf(unsigned level,
				optional<uint64_t> key,
				btree_detail::node_ref<ValueTraits> const &n) {
			if (already_visited(n))
				return false;

			check_sum(n);

			if (!key)
			  new_root(level);

			check_block_nr(n);
			check_max_entries(n);
			check_nr_entries(n, !key);
			check_ordered_keys(n);
			check_parent_key(key, n);
			check_leaf_key(level, n);
			return true;
		}

		boost::optional<error_set::ptr> get_errors() const {
			return errs_;
		}

	protected:
		block_counter &get_counter() {
			return counter_;
		}

	private:
		template <typename node>
		bool already_visited(node const &n) {
			block_address b = n.get_location();

			counter_.inc(b);

			if (seen_.count(b) > 0)
				return true;

			seen_.insert(b);
			return false;
		}

		template <typename node>
		void check_sum(node const &n) const {
			crc32c sum(BTREE_CSUM_XOR);

			disk_node const *data = n.raw();
			sum.append(&data->header.flags, MD_BLOCK_SIZE - sizeof(uint32_t));
			if (sum.get_sum() != n.get_checksum()) {
				std::ostringstream out;
				out << "checksum error for block " << n.get_block_nr()
				    << ", sum was " << sum.get_sum()
				    << ", expected " << n.get_checksum();
				errs_->add_child(out.str());
				throw runtime_error(out.str());
			}
		}

		template <typename node>
		void check_block_nr(node const &n) const {
			if (n.get_location() != n.get_block_nr()) {
				std::ostringstream out;
				out << "block number mismatch: actually "
				    << n.get_location()
				    << ", claims " << n.get_block_nr();
				errs_->add_child(out.str());
				throw runtime_error(out.str());
			}
		}

		template <typename node>
		void check_max_entries(node const &n) const {
			size_t elt_size = sizeof(uint64_t) + n.get_value_size();
			if (elt_size * n.get_max_entries() + sizeof(node_header) > MD_BLOCK_SIZE) {
				std::ostringstream out;
				out << "max entries too large: " << n.get_max_entries();
				errs_->add_child(out.str());
			}

			if (n.get_max_entries() % 3) {
				std::ostringstream out;
				out << "max entries is not divisible by 3: " << n.get_max_entries();
				errs_->add_child(out.str());
				throw runtime_error(out.str());
			}
		}

		template <typename node>
		void check_nr_entries(node const &n, bool is_root) const {
			if (n.get_nr_entries() > n.get_max_entries()) {
				std::ostringstream out;
				out << "bad nr_entries: "
				    << n.get_nr_entries() << " < "
				    << n.get_max_entries();
				errs_->add_child(out.str());
				throw std::runtime_error(out.str());
			}

			block_address min = n.get_max_entries() / 3;
			if (!is_root && (n.get_nr_entries() < min)) {
				ostringstream out;
				out << "too few entries in btree: "
				    << n.get_nr_entries()
				    << ", expected at least "
				    << min;
				errs_->add_child(out.str());
				throw runtime_error(out.str());
			}
		}

		template <typename node>
		void check_ordered_keys(node const &n) const {
			unsigned nr_entries = n.get_nr_entries();

			if (nr_entries == 0)
				return; // can only happen if a root node

			uint64_t last_key = n.key_at(0);

			for (unsigned i = 1; i < nr_entries; i++) {
				uint64_t k = n.key_at(i);
				if (k <= last_key) {
					ostringstream out;
					out << "keys are out of order, " << k << " <= " << last_key;
					throw runtime_error(out.str());
				}
				last_key = k;
			}
		}

		template <typename node>
		void check_parent_key(boost::optional<uint64_t> key, node const &n) const {
			if (!key)
				return;

			if (*key > n.key_at(0)) {
				ostringstream out;
				out << "parent key mismatch: parent was " << *key
				    << ", but lowest in node was " << n.key_at(0);
				throw runtime_error(out.str());
			}
		}

		template <typename node>
		void check_leaf_key(unsigned level, node const &n) {
			if (n.get_nr_entries() == 0)
				return; // can only happen if a root node

			if (last_leaf_key_[level] && *last_leaf_key_[level] >= n.key_at(0)) {
				ostringstream out;
				out << "the last key of the previous leaf was " << *last_leaf_key_[level]
				    << " and the first key of this leaf is " << n.key_at(0);
				throw runtime_error(out.str());
			}

			last_leaf_key_[level] = n.key_at(n.get_nr_entries() - 1);
		}

     	        void new_root(unsigned level) {
		  // we're starting a new subtree, so should
		  // reset the last_leaf value.
		  last_leaf_key_[level] = boost::optional<uint64_t>();
	        }

		block_counter &counter_;
		std::set<block_address> seen_;
		error_set::ptr errs_;
		boost::optional<uint64_t> last_leaf_key_[Levels];
	};
}

//----------------------------------------------------------------

#endif

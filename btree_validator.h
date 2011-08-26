#ifndef BTREE_VALIDATOR_H
#define BTREE_VALIDATOR_H

#include "btree.h"

#include "error_set.h"

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
#if 0
			else
				it->second++;
#endif
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
	template <uint32_t Levels, typename ValueTraits, uint32_t BlockSize>
	class btree_validator : public btree<Levels, ValueTraits, BlockSize>::visitor {
	public:
		btree_validator(block_counter &counter)
			: counter_(counter),
			  errs_(new error_set("btree errors")) {
		}

		bool visit_internal(unsigned level, bool is_root,
				    btree_detail::node_ref<uint64_traits, BlockSize> const &n) {
			if (already_visited(n))
				return false;

			check_block_nr(n);
			check_max_entries(n);
			check_nr_entries(n, is_root);
			return true;
		}

		bool visit_internal_leaf(unsigned level, bool is_root,
					 btree_detail::node_ref<uint64_traits, BlockSize> const &n) {
			if (already_visited(n))
				return false;

			check_block_nr(n);
			check_max_entries(n);
			check_nr_entries(n, is_root);
			return true;
		}

		bool visit_leaf(unsigned level, bool is_root,
				btree_detail::node_ref<ValueTraits, BlockSize> const &n) {
			if (already_visited(n))
				return false;

			check_block_nr(n);
			check_max_entries(n);
			check_nr_entries(n, is_root);
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
			if (elt_size * n.get_max_entries() + sizeof(node_header) > BlockSize) {
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

		block_counter &counter_;
		std::set<block_address> seen_;
		error_set::ptr errs_;
	};
}

//----------------------------------------------------------------

#endif

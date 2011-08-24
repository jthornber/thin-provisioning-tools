#ifndef BTREE_VALIDATOR_H
#define BTREE_VALIDATOR_H

#include "btree.h"

#include <sstream>
#include <map>
#include <set>

//----------------------------------------------------------------

namespace persistent_data {
	//----------------------------------------------------------------
	// Little helper class that keeps track of how many times blocks
	// are referenced.
	//----------------------------------------------------------------
	class block_counter {
	public:
		void inc(block_address b) {
			auto it = counts_.find(b);
			if (it == counts_.end())
				counts_.insert(make_pair(b, 1));
#if 0
			else
				it->second++;
#endif
		}

		unsigned get_count(block_address b) const {
			auto it = counts_.find(b);
			return (it == counts_.end()) ? 0 : it->second;
		}

		std::map<block_address, unsigned> const &get_counts() const {
			return counts_;
		}

	private:
		std::map<block_address, unsigned> counts_;
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
			: counter_(counter) {
		}

		void visit_internal(unsigned level, bool is_root,
				    btree_detail::node_ref<uint64_traits, BlockSize> const &n) {
			counter_.inc(n.get_location());
			check_duplicate_block(n.get_location());
			check_block_nr(n);
			check_max_entries(n);
			check_nr_entries(n, is_root);
		}

		void visit_internal_leaf(unsigned level, bool is_root,
					 btree_detail::node_ref<uint64_traits, BlockSize> const &n) {
			counter_.inc(n.get_location());
			check_duplicate_block(n.get_location());
			check_block_nr(n);
			check_max_entries(n);
			check_nr_entries(n, is_root);
		}

		void visit_leaf(unsigned level, bool is_root,
				btree_detail::node_ref<ValueTraits, BlockSize> const &n) {
			counter_.inc(n.get_location());
			check_duplicate_block(n.get_location());
			check_block_nr(n);
			check_max_entries(n);
			check_nr_entries(n, is_root);
		}

	private:
		void check_duplicate_block(block_address b) {
			if (seen_.count(b)) {
				std::ostringstream out;
				out << "duplicate block in btree: " << b;
				throw std::runtime_error(out.str());
			}

			seen_.insert(b);
		}

		template <typename node>
		void check_block_nr(node const &n) const {
			if (n.get_location() != n.get_block_nr()) {
				std::ostringstream out;
				out << "block number mismatch: actually "
				    << n.get_location()
				    << ", claims " << n.get_block_nr();
				throw std::runtime_error(out.str());
			}
		}

		template <typename node>
		void check_max_entries(node const &n) const {
			size_t elt_size = sizeof(uint64_t) + n.get_value_size();
			if (elt_size * n.get_max_entries() + sizeof(node_header) > BlockSize) {
				std::ostringstream out;
				out << "max entries too large: " << n.get_max_entries();
				throw std::runtime_error(out.str());
			}

			if (n.get_max_entries() % 3) {
				std::ostringstream out;
				out << "max entries is not divisible by 3: " << n.get_max_entries();
				throw std::runtime_error(out.str());
			}
		}

		template <typename node>
		void check_nr_entries(node const &n, bool is_root) const {
			if (n.get_nr_entries() > n.get_max_entries()) {
				std::ostringstream out;
				out << "bad nr_entries: "
				    << n.get_nr_entries() << " < "
				    << n.get_max_entries();
				throw std::runtime_error(out.str());
			}

			block_address min = n.get_max_entries() / 3;
			if (!is_root && (n.get_nr_entries() < min)) {
				ostringstream out;
				out << "too few entries in btree: "
				    << n.get_nr_entries()
				    << ", expected at least "
				    << min;
				throw runtime_error(out.str());
			}
		}

		block_counter &counter_;
		std::set<block_address> seen_;
	};
}

//----------------------------------------------------------------

#endif

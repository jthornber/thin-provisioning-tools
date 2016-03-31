#ifndef BTREE_NODE_CHECKER_H
#define BTREE_NODE_CHECKER_H

#include "block-cache/block_cache.h"
#include "persistent-data/block.h"
#include "persistent-data/data-structures/btree.h"
#include "persistent-data/data-structures/btree_disk_structures.h"

#include <boost/optional.hpp>
#include <string>

using bcache::block_address;

//----------------------------------------------------------------

namespace persistent_data {
	namespace btree_detail {
		class btree_node_checker {
		public:
			enum error_type {
				NO_ERROR,
				BLOCK_NR_MISMATCH,
				VALUE_SIZES_MISMATCH,
				MAX_ENTRIES_TOO_LARGE,
				MAX_ENTRIES_NOT_DIVISIBLE,
				NR_ENTRIES_TOO_LARGE,
				NR_ENTRIES_TOO_SMALL,
				KEYS_OUT_OF_ORDER,
				VALUE_SIZE_MISMATCH,
				PARENT_KEY_MISMATCH,
				LEAF_KEY_OVERLAPPED,
			};

			btree_node_checker():
				last_error_(NO_ERROR),
				error_location_(0),
				error_block_nr_(0),
				error_nr_entries_(0),
				error_max_entries_(0),
				error_value_sizes_{0, 0},
				error_keys_{0, 0} {
			}

			template <typename ValueTraits>
			bool check_block_nr(btree_detail::node_ref<ValueTraits> const &n) {
				if (n.get_location() != n.get_block_nr()) {
					last_error_ = BLOCK_NR_MISMATCH;
					error_block_nr_ = n.get_block_nr();
					error_location_ = n.get_location();

					return false;
				}

				return true;
			}

			template <typename ValueTraits>
			bool check_value_size(btree_detail::node_ref<ValueTraits> const &n) {
				if (!n.value_sizes_match()) {
					last_error_ = VALUE_SIZES_MISMATCH;
					error_location_ = n.get_location();
					error_value_sizes_[0] = n.get_value_size();
					error_value_sizes_[1] = sizeof(typename ValueTraits::disk_type);
					return false;
				}

				return true;
			}

			template <typename ValueTraits>
			bool check_max_entries(btree_detail::node_ref<ValueTraits> const &n) {
				size_t elt_size = sizeof(uint64_t) + n.get_value_size();
				if (elt_size * n.get_max_entries() + sizeof(node_header) > MD_BLOCK_SIZE) {
					last_error_ = MAX_ENTRIES_TOO_LARGE;
					error_location_ = n.get_location();
					error_max_entries_ = n.get_max_entries();

					return false;
				}

				if (n.get_max_entries() % 3) {
					last_error_ = MAX_ENTRIES_NOT_DIVISIBLE;
					error_location_ = n.get_location();
					error_max_entries_ = n.get_max_entries();

					return false;
				}

				return true;
			}

			template <typename ValueTraits>
			bool check_nr_entries(btree_detail::node_ref<ValueTraits> const &n,
					      bool is_root) {
				if (n.get_nr_entries() > n.get_max_entries()) {
					last_error_ = NR_ENTRIES_TOO_LARGE;
					error_location_ = n.get_location();
					error_nr_entries_ = n.get_nr_entries();
					error_max_entries_ = n.get_max_entries();

					return false;
				}

				block_address min = n.get_max_entries() / 3;
				if (!is_root && (n.get_nr_entries() < min)) {
					last_error_ = NR_ENTRIES_TOO_SMALL;
					error_location_ = n.get_location();
					error_nr_entries_ = n.get_nr_entries();
					error_max_entries_ = n.get_max_entries();

					return false;
				}

				return true;
			}

			template <typename ValueTraits>
			bool check_ordered_keys(btree_detail::node_ref<ValueTraits> const &n) {
				unsigned nr_entries = n.get_nr_entries();

				if (nr_entries == 0)
					return true; // can only happen if a root node

				uint64_t last_key = n.key_at(0);

				for (unsigned i = 1; i < nr_entries; i++) {
					uint64_t k = n.key_at(i);
					if (k <= last_key) {
						last_error_ = KEYS_OUT_OF_ORDER;
						error_location_ = n.get_location();
						error_keys_[0] = k;
						error_keys_[1] = last_key;

						return false;
					}
					last_key = k;
				}

				return true;
			}

			template <typename ValueTraits>
			bool check_parent_key(btree_detail::node_ref<ValueTraits> const &n,
					      boost::optional<uint64_t> key) {
				if (!key)
					return true;

				if (*key > n.key_at(0)) {
					last_error_ = PARENT_KEY_MISMATCH;
					error_location_ = n.get_location();
					error_keys_[0] = n.key_at(0);
					error_keys_[1] = *key;

					return false;
				}

				return true;
			}

			template <typename ValueTraits>
			bool check_leaf_key(btree_detail::node_ref<ValueTraits> const &n,
					    boost::optional<uint64_t> key) {
				if (n.get_nr_entries() == 0)
					return true; // can only happen if a root node

				if (key && *key >= n.key_at(0)) {
					last_error_ = LEAF_KEY_OVERLAPPED;
					error_location_ = n.get_location();
					error_keys_[0] = n.key_at(0);
					error_keys_[1] = *key;

					return false;
				}

				return true;
			}

			error_type get_last_error();
			std::string get_last_error_string();
			void reset();

		private:
			std::string block_nr_mismatch_string();
			std::string value_sizes_mismatch_string();
			std::string max_entries_too_large_string();
			std::string max_entries_not_divisible_string();
			std::string nr_entries_too_large_string();
			std::string nr_entries_too_small_string();
			std::string keys_out_of_order_string();
			std::string parent_key_mismatch_string();
			std::string leaf_key_overlapped_string();

			error_type last_error_;
			block_address error_location_;
			block_address error_block_nr_;
			uint32_t error_nr_entries_;
			uint32_t error_max_entries_;
			uint32_t error_value_sizes_[2];
			uint64_t error_keys_[2];
		};
	}
}

//----------------------------------------------------------------

#endif

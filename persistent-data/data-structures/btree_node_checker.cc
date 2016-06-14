#include "btree_node_checker.h"

#include <sstream>

using persistent_data::btree_detail::btree_node_checker;

//----------------------------------------------------------------

btree_node_checker::error_type btree_node_checker::get_last_error() const {
	return last_error_;
}

std::string btree_node_checker::get_last_error_string() const {
	switch (last_error_) {
	case BLOCK_NR_MISMATCH:
		return block_nr_mismatch_string();
	case VALUE_SIZES_MISMATCH:
		return value_sizes_mismatch_string();
	case MAX_ENTRIES_TOO_LARGE:
		return max_entries_too_large_string();
	case MAX_ENTRIES_NOT_DIVISIBLE:
		return max_entries_not_divisible_string();
	case NR_ENTRIES_TOO_LARGE:
		return nr_entries_too_large_string();
	case NR_ENTRIES_TOO_SMALL:
		return nr_entries_too_small_string();
	case KEYS_OUT_OF_ORDER:
		return keys_out_of_order_string();
	case PARENT_KEY_MISMATCH:
		return parent_key_mismatch_string();
	case LEAF_KEY_OVERLAPPED:
		return leaf_key_overlapped_string();
	default:
		return std::string();
	}
}

void btree_node_checker::reset() {
	last_error_ = NO_ERROR;
}

std::string btree_node_checker::block_nr_mismatch_string() const {
	std::ostringstream out;
	out << "block number mismatch: actually "
	    << error_location_
	    << ", claims " << error_block_nr_;

	return out.str();
}

std::string btree_node_checker::value_sizes_mismatch_string() const {
	std::ostringstream out;
	out << "value size mismatch: expected " << error_value_sizes_[1]
	    << ", but got " << error_value_sizes_[0]
	    << ". This is not the btree you are looking for."
	    << " (block " << error_location_ << ")";

	return out.str();
}

std::string btree_node_checker::max_entries_too_large_string() const {
	std::ostringstream out;
	out << "max entries too large: " << error_max_entries_
	    << " (block " << error_location_ << ")";

	return out.str();
}

std::string btree_node_checker::max_entries_not_divisible_string() const {
	std::ostringstream out;
	out << "max entries is not divisible by 3: " << error_max_entries_
	    << " (block " << error_location_ << ")";

	return out.str();
}

std::string btree_node_checker::nr_entries_too_large_string() const {
	std::ostringstream out;
	out << "bad nr_entries: "
	    << error_nr_entries_ << " < "
	    << error_max_entries_
	    << " (block " << error_location_ << ")";

	return out.str();
}

std::string btree_node_checker::nr_entries_too_small_string() const {
	std::ostringstream out;
	out << "too few entries in btree_node: "
	    << error_nr_entries_
	    << ", expected at least "
	    << (error_max_entries_ / 3)
	    << " (block " << error_location_
	    << ", max_entries = " << error_max_entries_ << ")";

	return out.str();
}

std::string btree_node_checker::keys_out_of_order_string() const {
	std::ostringstream out;
	out << "keys are out of order, "
	    << error_keys_[0] << " <= " << error_keys_[1]
	    << " (block " << error_location_ << ")";

	return out.str();
}

std::string btree_node_checker::parent_key_mismatch_string() const {
	std::ostringstream out;
	out << "parent key mismatch: parent was " << error_keys_[1]
	    << ", but lowest in node was " << error_keys_[0]
	    << " (block " << error_location_ << ")";

	return out.str();
}

std::string btree_node_checker::leaf_key_overlapped_string() const {
	std::ostringstream out;
	out << "the last key of the previous leaf was " << error_keys_[1]
	    << " and the first key of this leaf is " << error_keys_[0]
	    << " (block " << error_location_ << ")";

	return out.str();
}

//----------------------------------------------------------------


// Copyright (C) 2020 Red Hat, Inc. All rights reserved.
//
// This file is part of the thin-provisioning-tools source.
//
// thin-provisioning-tools is free software: you can redistribute it
// and/or modify it under the terms of the GNU General Public License
// as published by the Free Software Foundation, either version 3 of
// the License, or (at your option) any later version.
//
// thin-provisioning-tools is distributed in the hope that it will be
// useful, but WITHOUT ANY WARRANTY; without even the implied warranty
// of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License along
// with thin-provisioning-tools.  If not, see
// <http://www.gnu.org/licenses/>.

#include "persistent-data/space-maps/core.h"
#include "persistent-data/math_utils.h"

#include <stdexcept>

using namespace persistent_data;
using namespace std;

//----------------------------------------------------------------

namespace {
	constexpr block_address ENTRIES_PER_WORD = 4 * sizeof(uint64_t);

	class core_map : public checked_space_map {
	public:
		typedef std::shared_ptr<core_map> ptr;

		core_map(block_address nr_blocks);

		block_address get_nr_blocks() const override;
		block_address get_nr_free() const override;
		ref_t get_count(block_address b) const override;
		void set_count(block_address b, ref_t c) override;
		void commit() override;
		void inc(block_address b, ref_t count) override;
		void dec(block_address b, ref_t count) override;
		maybe_block find_free(span_iterator &it) override;
		void extend(block_address extra_blocks) override;
		void count_metadata(block_counter &bc) const override;

		// FIXME: meaningless, but this class is only used for testing
		size_t root_size() const override;
		
		// FIXME: meaningless, but this class is only used for testing
		virtual void copy_root(void *dest, size_t len) const override;

		checked_space_map::ptr clone() const override;
	private:
		ref_t get_count_(block_address b) const;
		void set_count_(block_address b, ref_t c);
		void check_block_(block_address b) const;
		ref_t lookup_bits_(block_address b) const;
		void set_bits_(block_address b, ref_t c);
		ref_t lookup_exception_(block_address b) const;

		block_address nr_blocks_;
		block_address nr_free_;
		block_address search_start_;

		std::vector<uint64_t> bits_;
		std::map<block_address, ref_t> exceptions_;

	};

	core_map::core_map(block_address nr_blocks)
		: nr_blocks_(nr_blocks),
		  nr_free_(nr_blocks),
		  search_start_(0),
		  bits_(base::div_up(nr_blocks, ENTRIES_PER_WORD)) {
	}

	block_address
	core_map::get_nr_blocks() const {
		return nr_blocks_;
	}

	block_address
	core_map::get_nr_free() const {
		return nr_free_;
	}

	ref_t
	core_map::get_count(block_address b) const {
		check_block_(b);
		return get_count_(b);
	}

	void
	core_map::set_count(block_address b, ref_t c) {
		check_block_(b);
		set_count_(b, c);
	}

	void
	core_map::commit() {
	}

	void
	core_map::inc(block_address b, ref_t count) {
		check_block_(b);
		auto old_c = get_count_(b);
		set_count_(b, old_c + count);
	}

	void
	core_map::dec(block_address b, ref_t count) {
		check_block_(b);
		auto old_c = get_count_(b);
		set_count_(b, old_c - count);
	}

	core_map::maybe_block
	core_map::find_free(span_iterator &it) {
		for (maybe_span ms = it.first(); ms; ms = it.next()) {
			for (block_address b = std::max(search_start_, ms->first); b < ms->second; b++) {
				check_block_(b);
				if (!get_count_(b))
					return maybe_block(b);
			}
		}

		return maybe_block();
	}

	void
	core_map::extend(block_address extra_blocks) {
		throw std::runtime_error("'extend' not implemented");
	}

	void
	core_map::count_metadata(block_counter &bc) const {
	}

	// FIXME: meaningless, but this class is only used for testing
	size_t
	core_map::root_size() const {
		return 0;
	}

	// FIXME: meaningless, but this class is only used for testing
	void
	core_map::copy_root(void *dest, size_t len) const {
		throw std::runtime_error("'copy root' not implemented");
	}

	checked_space_map::ptr
	core_map::clone() const {
		return ptr(new core_map(*this));
	}

	void
	core_map::check_block_(block_address b) const {
		if (b >= nr_blocks_)
			throw std::runtime_error("block out of bounds");
	}

	ref_t
	core_map::get_count_(block_address b) const {
		auto c = lookup_bits_(b);
		if (c == 3)
			c = lookup_exception_(b);

		return c;
	}

	void
	core_map::set_count_(block_address b, ref_t c) {
		auto old_c = get_count_(b);
		
		if (old_c > 2) {
			if (c > 2)
				exceptions_[b] = c;
			else {
				exceptions_.erase(b);
				set_bits_(b, c);
			}
		} else {
			if (c > 2) {
				set_bits_(b, 3);
				exceptions_.insert(make_pair(b, c));
			} else
				set_bits_(b, c);
		}
		
		if (old_c == 0 && c > 0)
			nr_free_--;

		else if (old_c > 0 && c == 0) {
			if (b < search_start_)
				search_start_ = b;

			nr_free_++;
		}
	}

	ref_t
	core_map::lookup_bits_(block_address b) const {
		block_address word = b / ENTRIES_PER_WORD;
		block_address shift = (b % ENTRIES_PER_WORD) * 2;

		return (bits_[word] >> shift) & 0b11;
	}

	void
	core_map::set_bits_(block_address b, ref_t c) {
		if (c > 3)
			throw runtime_error("bad count");

		block_address word = b / ENTRIES_PER_WORD;
		block_address shift = (b % ENTRIES_PER_WORD) * 2ull;

		auto w = bits_[word] & ~(0b11ull << shift);
		bits_[word] = w | (((uint64_t) c) << shift);
	}
	
	ref_t
	core_map::lookup_exception_(block_address b) const {
		auto it = exceptions_.find(b);
		if (it == exceptions_.end())
			throw runtime_error("core space map exception entry missing");

		return it->second;
	}
}

//----------------------------------------------------------------

checked_space_map::ptr
persistent_data::create_core_map(block_address nr_blocks)
{
	return checked_space_map::ptr(new core_map(nr_blocks));
}

//----------------------------------------------------------------

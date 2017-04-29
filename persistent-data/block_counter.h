// Copyright (C) 2011 Red Hat, Inc. All rights reserved.
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

#ifndef BLOCK_COUNTER_H
#define BLOCK_COUNTER_H

#include "block.h"
#include "run_set.h"

//----------------------------------------------------------------

namespace persistent_data {
	//----------------------------------------------------------------
	// Little helper class that keeps track of how many times blocks
	// are referenced.
	//----------------------------------------------------------------
	class block_counter {
	public:
		typedef std::map<block_address, unsigned> count_map;

		virtual ~block_counter() {}

		virtual void inc(block_address b) {
			count_map::iterator it = counts_.find(b);
			if (it == counts_.end())
				counts_.insert(std::make_pair(b, 1));
			else
				it->second++;
		}

		virtual unsigned get_count(block_address b) const {
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
	// Little helper class that keeps track of which blocks
	// are referenced.
	//----------------------------------------------------------------
	class binary_block_counter : public block_counter {
	public:
		virtual ~binary_block_counter() {}

		virtual void inc(block_address b) {
			visited_.add(b);
		}

		virtual unsigned get_count(block_address b) const {
			return visited_.member(b) ? 1 : 0;
		}

		base::run_set<block_address> const& get_visited() const {
			return visited_;
		}
	private:
		base::run_set<block_address> visited_;
	};
}

//----------------------------------------------------------------

#endif

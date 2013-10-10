// Copyright (C) 2013 Red Hat, Inc. All rights reserved.
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

#ifndef BITSET_H
#define BITSET_H

#include "persistent-data/run.h"

//----------------------------------------------------------------

namespace persistent_data {
	namespace bitset_detail {
		class bitset_impl;

		class missing_bits {
		public:
			missing_bits(base::run<uint32_t> const &keys)
				: keys_(keys) {
			}

			base::run<uint32_t> keys_;
		};

		class bitset_visitor {
		public:
			typedef boost::shared_ptr<bitset_visitor> ptr;

			virtual ~bitset_visitor();
			virtual void visit(uint32_t index, bool value);
			virtual void visit(missing_bits const &d);
		};
	}

	class bitset {
	public:
		typedef boost::shared_ptr<bitset> ptr;
		typedef typename persistent_data::transaction_manager::ptr tm_ptr;

		bitset(tm_ptr tm);
		bitset(tm_ptr tm, block_address root, unsigned nr_bits);
		block_address get_root() const;
		void grow(unsigned new_nr_bits, bool default_value);
		void destroy();

		// May trigger a flush, so cannot be const
		bool get(unsigned n);
		void set(unsigned n, bool value);
		void flush();

		void walk_bitset(bitset_detail::bitset_visitor &v) const;

	private:
		boost::shared_ptr<bitset_detail::bitset_impl> impl_;
	};
}

//----------------------------------------------------------------

#endif

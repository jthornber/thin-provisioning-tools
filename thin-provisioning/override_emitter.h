// Copyright (C) 2019 Red Hat, Inc. All rights reserved.
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

#ifndef TP_OVERRIDE_EMITTER_H
#define TP_OVERRIDE_EMITTER_H

#include "emitter.h"
#include "metadata.h"

#include <boost/optional.hpp>
#include <sstream>

//----------------------------------------------------------------

namespace thin_provisioning {
	struct override_error : public std::runtime_error {
		override_error(std::string const &str)
			: std::runtime_error(str) {
			}
	};

	struct override_options {
		uint64_t get_transaction_id() const {
			if (!transaction_id_)
				bad_override_("transaction id");

			return *transaction_id_;
		}

		uint64_t get_transaction_id(uint64_t dflt) const {
			return transaction_id_ ? *transaction_id_ : dflt;
		}

		uint32_t get_data_block_size() const {
			if (!data_block_size_)
				bad_override_("data block size");

			return *data_block_size_;
		}

		uint32_t get_data_block_size(uint32_t dflt) const {
			return data_block_size_ ? *data_block_size_ : dflt;
		}

		uint64_t get_nr_data_blocks() const {
			if (!nr_data_blocks_)
				bad_override_("nr data blocks");

			return *nr_data_blocks_;
		}

		uint64_t get_nr_data_blocks(uint64_t dflt) const {
			return nr_data_blocks_ ? *nr_data_blocks_ : dflt;
		}

		boost::optional<uint64_t> transaction_id_;
		boost::optional<uint32_t> data_block_size_;
		boost::optional<uint64_t> nr_data_blocks_;

	private:
		void bad_override_(std::string const &field) const {
			throw override_error(field);
		}
	};

	emitter::ptr create_override_emitter(emitter::ptr inner, override_options const &opts);
}

//----------------------------------------------------------------

#endif

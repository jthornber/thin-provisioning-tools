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

#include "human_readable_format.h"

#include <iostream>

using namespace std;
using namespace thin_provisioning;

//----------------------------------------------------------------

namespace {
	template <typename T>
	std::ostream &operator << (ostream &out, boost::optional<T> const &maybe) {
		if (maybe)
			out << *maybe;

		return out;
	}

	class hr_emitter : public emitter {
	public:
		hr_emitter(ostream &out)
			: out_(out) {
		}

		void begin_superblock(string const &uuid,
				      uint64_t time,
				      uint64_t trans_id,
				      uint32_t data_block_size,
				      uint64_t nr_data_blocks,
				      boost::optional<uint64_t> metadata_snap) {
			out_ << "begin superblock: \"" << uuid << "\""
			     << ", " << time
			     << ", " << trans_id
			     << ", " << data_block_size
			     << ", " << nr_data_blocks;
			if (metadata_snap)
				out_ << ", " << metadata_snap;

			out_ << endl;
		}

		void end_superblock() {
			out_ << "end superblock" << endl;
		}

		void begin_device(uint32_t dev_id,
				  uint64_t mapped_blocks,
				  uint64_t trans_id,
				  uint64_t creation_time,
				  uint64_t snap_time) {
			out_ << "device: " << dev_id << endl
			     << "mapped_blocks: " << mapped_blocks << endl
			     << "transaction: " << trans_id << endl
			     << "creation time: " << creation_time << endl
			     << "snap time: " << snap_time << endl;
		}

		void end_device() {
			out_ << endl;
		}

		void begin_named_mapping(string const &name) {
			out_ << "begin named mapping"
			     << endl;
		}

		void end_named_mapping() {
			out_ << "end named mapping"
			     << endl;
		}

		void identifier(string const &name) {
			out_ << "identifier: " << name << endl;
		}

		void range_map(uint64_t origin_begin, uint64_t data_begin, uint32_t time, uint64_t len) {
			out_ << "    (" << origin_begin
			     << ".." << origin_begin + len - 1
			     << ") -> (" << data_begin
			     << ".." << data_begin + len - 1
			     << "), "
			     << time
			     << endl;
		}

		void single_map(uint64_t origin_block, uint64_t data_block, uint32_t time) {
			out_ << "    " << origin_block
			     << " -> " << data_block
			     << ", " << time
			     << endl;
		}

	private:
		ostream &out_;
	};
}

//----------------------------------------------------------------

thin_provisioning::emitter::ptr
thin_provisioning::create_human_readable_emitter(ostream &out)
{
	return emitter::ptr(new hr_emitter(out));
}

//----------------------------------------------------------------

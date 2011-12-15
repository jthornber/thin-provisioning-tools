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

#ifndef EMITTER_H
#define EMITTER_H

#include <boost/shared_ptr.hpp>
#include <string>
#include <stdint.h>

//----------------------------------------------------------------

namespace thin_provisioning {

	//------------------------------------------------
	// Here's a little grammar for how this all hangs together:
	//
	// superblock := <uuid> <time> <trans_id> <data_block_size> device*
	// device := <dev id> <transaction id> <creation time> <snap time> <binding>
        // binding := (<identifier> | <mapping>)*
	// mapping := range_map | single_map
	// range_map := <origin_begin> <origin_end> <data_begin>
	// single_map := <origin> <data>
	// named_mapping := <identifier> <mapping>
	//------------------------------------------------
	class emitter {
	public:
		typedef boost::shared_ptr<emitter> ptr;

		virtual ~emitter() {}

		virtual void begin_superblock(std::string const &uuid,
					      uint64_t time,
					      uint64_t trans_id,
					      uint32_t data_block_size) = 0;
		virtual void end_superblock() = 0;

		virtual void begin_device(uint32_t dev_id,
					  uint64_t mapped_blocks,
					  uint64_t trans_id,
					  uint64_t creation_time,
					  uint64_t snap_time) = 0;
		virtual void end_device() = 0;

		virtual void begin_named_mapping(std::string const &name) = 0;
		virtual void end_named_mapping() = 0;

		virtual void identifier(std::string const &name) = 0;
		virtual void range_map(uint64_t origin_begin, uint64_t data_begin, uint32_t time, uint64_t len) = 0;
		virtual void single_map(uint64_t origin_block, uint64_t data_block, uint32_t time) = 0;
	};
}

//----------------------------------------------------------------

#endif

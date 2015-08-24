// Copyright (C) 2015 Red Hat, Inc. All rights reserved.
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

#ifndef CHUNK_STREAM_H
#define CHUNK_STREAM_H

#include "block-cache/block_cache.h"

#include <deque>
#include <stdint.h>

//----------------------------------------------------------------

namespace thin_provisioning {
	struct mem {
		mem(uint8_t *b, uint8_t *e)
			: begin(b),
			  end(e) {
		}

		uint8_t *begin, *end;
	};

	struct chunk {
		// FIXME: switch to bytes rather than sectors
		// FIXME: add length too
		uint64_t offset_sectors_;
		std::deque<mem> mem_;
	};

	class chunk_stream {
	public:
		virtual ~chunk_stream() {}

		virtual bcache::block_address nr_chunks() const = 0;
		virtual void rewind() = 0;
		virtual bool advance(bcache::block_address count = 1ull) = 0;
		virtual bcache::block_address index() const = 0;
		virtual chunk const &get() const = 0;
	};
}

//----------------------------------------------------------------

#endif

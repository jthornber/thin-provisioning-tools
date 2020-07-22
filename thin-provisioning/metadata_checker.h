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

#ifndef METADATA_CHECKER_H
#define METADATA_CHECKER_H

#include "base/error_state.h"
#include "block-cache/block_cache.h"
#include "persistent-data/block.h"

//----------------------------------------------------------------

namespace thin_provisioning {
	struct check_options {
		enum data_mapping_options {
			DATA_MAPPING_NONE,
			DATA_MAPPING_LEVEL1,
			DATA_MAPPING_LEVEL2,
		};

		enum space_map_options {
			SPACE_MAP_NONE,
			SPACE_MAP_FULL,
		};

		check_options();

		void set_superblock_only();
		void set_skip_mappings();
		void set_override_mapping_root(bcache::block_address b);
		void set_metadata_snap();
		void set_ignore_non_fatal();

		bool use_metadata_snap_;
		data_mapping_options check_data_mappings_;
		space_map_options sm_opts_;
		boost::optional<bcache::block_address> override_mapping_root_;
		bool ignore_non_fatal_;
	};

	enum output_options {
		OUTPUT_NORMAL,
		OUTPUT_QUIET,
	};

	base::error_state
	check_metadata(persistent_data::block_manager::ptr bm,
		       check_options const &check_opts,
		       output_options output_opts);
}

//----------------------------------------------------------------

#endif

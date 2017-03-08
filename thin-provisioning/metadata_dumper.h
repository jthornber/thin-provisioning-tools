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

#ifndef METADATA_DUMPER_H
#define METADATA_DUMPER_H

#include "emitter.h"
#include "metadata.h"

#include <boost/optional.hpp>
#include <set>

//----------------------------------------------------------------

namespace thin_provisioning {
	class dump_options {
	public:
		dump_options()
			: repair_(false),
			  skip_mappings_(false) {
		}

		bool selected_dev(uint64_t dev_id) const {
			return !dev_filter_ || dev_filter_->count(dev_id);
		}

		void select_dev(uint64_t dev_id) {
			if (!dev_filter_)
				dev_filter_ = dev_set();

			dev_filter_->insert(dev_id);
		}

		bool repair_;
		bool skip_mappings_;

		using dev_set = std::set<uint64_t>;
		using maybe_dev_set = boost::optional<dev_set>;
		maybe_dev_set dev_filter_;
	};

	// Set the @repair flag if your metadata is corrupt, and you'd like
	// the dumper to do it's best to recover info.  If not set, any
	// corruption encountered will cause an exception to be thrown.
	void metadata_dump(metadata::ptr md, emitter::ptr e, dump_options const &opts);
	void metadata_dump_subtree(metadata::ptr md, emitter::ptr e, bool repair, uint64_t subtree_root);
}

//----------------------------------------------------------------

#endif

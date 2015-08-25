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

#include "thin-provisioning/pool_stream.h"
#include "persistent-data/data-structures/btree_damage_visitor.h"

using namespace thin_provisioning;
using namespace persistent_data;

//----------------------------------------------------------------

namespace {
	class damage_visitor {
	public:
		virtual void visit(btree_path const &path, btree_detail::damage const &d) {
			throw std::runtime_error("damage in mapping tree, please run thin_check");
		}
	};

	uint32_t const UNMAPPED = -1;
}

//----------------------------------------------------------------

pool_stream::pool_stream(cache_stream &stream,
			 transaction_manager::ptr tm, superblock_detail::superblock const &sb,
			 block_address nr_blocks)
	: stream_(stream),
	  block_to_thin_(stream.nr_chunks(), UNMAPPED),
	  nr_mapped_(0)
{
	init_rmap(tm, sb, nr_blocks);
}

block_address
pool_stream::nr_chunks() const
{
	return nr_mapped_;
}

void
pool_stream::rewind()
{
	stream_.rewind();
}

bool
pool_stream::advance(block_address count)
{
	while (count--)
		if (!advance_one())
			return false;

	return true;
}

block_address
pool_stream::index() const
{
	return stream_.index();
}

chunk const &
pool_stream::get() const
{
	return stream_.get();
}



// FIXME: too big to return by value
vector<pool_stream::rmap_region>
pool_stream::read_rmap(transaction_manager::ptr tm,
		       superblock_detail::superblock const &sb,
		       block_address nr_blocks)
{
	damage_visitor dv;
	rmap_visitor rv;

	mapping_tree mtree(*tm, sb.data_mapping_root_,
			   mapping_tree_detail::block_traits::ref_counter(tm->get_sm()));

	rv.add_data_region(rmap_visitor::region(0, nr_blocks));

	btree_visit_values(mtree, rv, dv);
	rv.complete();
	cerr << "rmap size: " << rv.get_rmap().size() << "\n";
	return rv.get_rmap();
}

void
pool_stream::init_rmap(transaction_manager::ptr tm,
		       superblock_detail::superblock const &sb,
		       block_address nr_blocks)
{
	cerr << "reading rmap...";
	vector<rmap_region> rmap = read_rmap(tm, sb, nr_blocks);
	cerr << "done\n";

	vector<rmap_region>::const_iterator it;
	set<uint32_t> thins;
	for (it = rmap.begin(); it != rmap.end(); ++it) {
		rmap_region const &r = *it;
		for (block_address b = r.data_begin; b != r.data_end; b++)
			if (block_to_thin_[b] == UNMAPPED) {
				nr_mapped_++;
				block_to_thin_[b] = r.thin_dev;
			}
		thins.insert(r.thin_dev);
	}

	cerr << nr_mapped_ << " mapped blocks\n";
	cerr << "there are " << thins.size() << " thin devices\n";
}

bool
pool_stream::advance_one()
{
	block_address new_index = index() + 1;

	while (block_to_thin_[new_index] == UNMAPPED &&
	       new_index < nr_chunks())
		new_index++;

	if (new_index >= nr_chunks())
		return false;

	return stream_.advance(new_index - index());
}

//----------------------------------------------------------------

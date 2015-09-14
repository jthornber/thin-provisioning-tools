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

#include "chunker/pool_stream.h"
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
	  block_to_thin_(nr_blocks, UNMAPPED),
	  nr_mapped_(0),
	  index_(0),
	  block_size_(sb.data_block_size_ * 512)
{
	init_rmap(tm, sb, nr_blocks);
}

block_address
pool_stream::size() const
{
	return nr_mapped_ * block_size_;
}

void
pool_stream::rewind()
{
	stream_.rewind();
	index_ = 0;
}

bool
pool_stream::next(block_address count)
{
	while (count--)
		if (!advance_one())
			return false;

	return true;
}

bool
pool_stream::eof() const
{
	return stream_.eof();
}

chunk const &
pool_stream::get()
{
	return stream_.get();
}

void
pool_stream::put(chunk const &c)
{
	stream_.put(c);
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
	block_address count = 1;

	while (((index_ + count) < block_to_thin_.size()) &&
	       (block_to_thin_[index_ + count] == UNMAPPED))
		count++;

	index_ += count;
	return stream_.next(count);
}

//----------------------------------------------------------------

#include "thin-provisioning/rmap_visitor.h"

#include <algorithm>
#include <iostream>

using namespace thin_provisioning;

//----------------------------------------------------------------

rmap_visitor::rmap_visitor()
{
}

void
rmap_visitor::add_data_region(region const &r)
{
	regions_.push_back(r);
}

void
rmap_visitor::visit(btree_path const &path,
		    mapping_tree_detail::block_time const &bt)
{
	if (in_regions(bt.block_)) {
		uint32_t thin_dev = path[0];
		block_address thin_block = path[1];

		visit_block(thin_dev, thin_block, bt.block_);
	}
}

namespace {
	bool cmp_data_begin(rmap_visitor::rmap_region const &lhs,
			    rmap_visitor::rmap_region const &rhs) {
		return lhs.data_begin < rhs.data_begin;
	};
}

void
rmap_visitor::complete()
{
	if (current_rmap_)
		push_current();

	std::sort(rmap_.begin(), rmap_.end(), cmp_data_begin);
}

vector<rmap_visitor::rmap_region> const &
rmap_visitor::get_rmap() const
{
	return rmap_;
}

// Slow, but I suspect we wont run with many regions.
bool
rmap_visitor::in_regions(block_address b) const
{
	vector<region>::const_iterator it;
	for (it = regions_.begin(); it != regions_.end(); ++it)
		if (it->contains(b))
			return true;

	return false;
}

bool
rmap_visitor::adjacent_block(rmap_region const &rr,
			     uint32_t thin_dev, block_address thin_block,
			     block_address data_block) const
{
	block_address run_length = rr.data_end - rr.data_begin;

	return (rr.thin_dev == thin_dev) &&
		(data_block == rr.data_end) &&
		(thin_block == rr.thin_begin + run_length);
}

void
rmap_visitor::insert_new_region(uint32_t thin_dev, block_address thin_block,
				block_address data_block)
{
	rmap_region rr;
	rr.data_begin = data_block;
	rr.data_end = data_block + 1;
	rr.thin_dev = thin_dev;
	rr.thin_begin = thin_block;

	current_rmap_ = rr;
}

void
rmap_visitor::push_current()
{
	rmap_.push_back(*current_rmap_);
	current_rmap_ = boost::optional<rmap_region>();
}

void
rmap_visitor::visit_block(uint32_t thin_dev, block_address thin_block,
			  block_address data_block)
{
	if (current_rmap_) {
		if (adjacent_block(*current_rmap_, thin_dev, thin_block, data_block))
			current_rmap_->data_end++;
		else {
			push_current();
			insert_new_region(thin_dev, thin_block, data_block);
		}

	} else
		insert_new_region(thin_dev, thin_block, data_block);
}

//----------------------------------------------------------------

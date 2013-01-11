// Copyright (C) 2012 Red Hat, Inc. All rights reserved.
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

using namespace base;
using namespace persistent_data;

//----------------------------------------------------------------

namespace {
	uint32_t const ARRAY_CSUM_XOR = 595846735;

	struct array_block_validator : public block_manager<>::validator {
		virtual void check(buffer<> const &b, block_address location) const {
			array_block_disk const *data = reinterpret_cast<array_block_disk const *>(&b);
			crc32c sum(ARRAY_CSUM_XOR);
			sum.append(&data->max_entries, MD_BLOCK_SIZE - sizeof(uint32_t));
			if (sum.get_sum() != to_cpu<uint32_t>(data->csum))
				throw checksum_error("bad checksum in array block node");

			if (to_cpu<uint64_t>(data->blocknr) != location)
				throw checksum_error("bad block nr in array block");
		}

		virtual void prepare(buffer<> &b, block_address location) const {
			array_block_disk *data = reinterpret_cast<array_block_disk *>(&b);
			data->blocknr = to_disk<base::__le64, uint64_t>(location);

			crc32c sum(ARRAY_CSUM_XOR);
			sum.append(&data->max_entries, MD_BLOCK_SIZE - sizeof(uint32_t));
			data->csum = to_disk<base::__le32>(sum.get_sum());
		}
	};

	struct array_dim {
		array_dim(unsigned nr_entries, unsigned entries_per_block)
			: nr_full_blocks(nr_entries / entries_per_block),
			  nr_entries_in_last_block(nr_entries % entries_per_block) {
		}

		unsigned nr_full_blocks;
		unsigned nr_entries_in_last_block;
	};

	unsigned calc_max_entries(size_t value_size, size_t block_size)
	{
		return (block_size - sizeof(struct array_block_disk)) / value_size;
	}
}

//----------------------------------------------------------------

template <typename ValueTraits>
array<ValueTraits>::array(typename persistent_data::transaction_manager::ptr tm,
			  typename ValueTraits::ref_counter rc,
			  unsigned nr_entries,
			  value_type const &default_value)
	: tm_(tm),
	  destroy_(false),
	  block_tree_(tm, array_block_traits<ValueTraits>()),
	  entries_per_block_(calc_max_entries(sizeof(value_type), MD_BLOCK_SIZE)),
	  rc_(rc)
{
}

template <typename ValueTraits>
array<ValueTraits>::array(typename persistent_data::transaction_manager::ptr tm,
			  typename ValueTraits::ref_counter rc,
			  block_address root)
	: tm_(tm),
	  destroy_(false),
	  block_tree_(tm, root, array_block_traits<ValueTraits>()),
	  entries_per_block_(calc_max_entries(sizeof(value_type), MD_BLOCK_SIZE)),
	  rc_(rc)
{
}

template <typename ValueTraits>
void
array<ValueTraits>::set_root(block_address root)
{
	block_tree_.set_root(root);
}

template <typename ValueTraits>
block_address
array<ValueTraits>::get_root() const
{
	return block_tree_.get_root();
}

template <typename ValueTraits>
void
array<ValueTraits>::destroy()
{
	block_tree_.destroy();
}

template <typename ValueTraits>
void
array<ValueTraits>::grow(unsigned old_size, unsigned new_size,
			 typename ValueTraits::value_type const &v)
{
	array_dim old_dim(old_size, entries_per_block_);
	array_dim new_dim(new_size, entries_per_block_);

	if (new_dim.nr_full_blocks > old_dim.nr_full_blocks) {
                if (old_dim.nr_entries_in_last_block > 0) {
			array_block<ValueTraits> ab = shadow_ablock(old_dim.nr_full_blocks);
                        fill_tail_block(ab, v, entries_per_block_);
                }

                insert_full_blocks(old_dim.nr_full_blocks, new_dim.nr_full_blocks + 1, v);
                insert_tail_block(new_dim.nr_full_blocks, new_dim.nr_entries_in_last_block, v);
        } else {
		array_block<ValueTraits> ab = get_ablock(new_dim.nr_full_blocks - 1u);
		fill_tail_block(ab, v, new_dim.nr_entries_in_last_block);
	}
}

template <typename ValueTraits>
void
array<ValueTraits>::shrink(unsigned old_size, unsigned new_size)
{

}

template <typename ValueTraits>
typename array<ValueTraits>::value_type const &
array<ValueTraits>::get(unsigned index) const
{
	array_block<ValueTraits> ab = get_ablock(index / entries_per_block_);
	return ab.get(index % entries_per_block_);
}

template <typename ValueTraits>
void
array<ValueTraits>::set(unsigned index, value_type const &value)
{
	array_block<ValueTraits> ab = shadow_ablock(index / entries_per_block_);
	ab.set(index % entries_per_block_, value);
}

template <typename ValueTraits>
ro_array_block<ValueTraits>
array<ValueTraits>::get_ablock(unsigned block_index) const
{
	return ro_array_block<ValueTraits>(tm_->read_lock(block_index));
}

template <typename ValueTraits>
array_block<ValueTraits>
array<ValueTraits>::shadow_ablock(unsigned block_index)
{
	typedef typename block_manager<>::write_ref write_ref;

	transaction_manager::validator v(new array_block_validator);
	std::pair<write_ref, bool> p = tm_->shadow(block_index, v);
	array_block<ValueTraits> ab(p.first);

	if (p.second)
		ab.inc_all_entries();

	uint64_t key[1];
	key[0] = block_index;
	block_tree_.insert(key, ab.get_location());

	return ab;
}

template <typename ValueTraits>
void
array<ValueTraits>::fill_tail_block(array_block<ValueTraits> &ab,
				    value_type v,
				    unsigned nr_entries)
{
	for (unsigned i = ab.nr_entries(); i < nr_entries; i++)
		ab.set(i, v);
}

template <typename ValueTraits>
void
array<ValueTraits>::insert_full_blocks(unsigned begin_index,
				       unsigned end_index,
				       value_type v)
{
	array_block<ValueTraits> ab = new_ablock();
	space_map::ptr sm = tm_->get_sm();

	for (unsigned i = 0; i < entries_per_block_; i++)
		ab.set(i, v);

	for (uint64_t b = begin_index; b < end_index; b++) {
		block_tree_.insert(b, ab);
		sm->inc(ab.address());
	}

	sm->dec(ab.adress());
}

template <typename ValueTraits>
void
array<ValueTraits>::insert_tail_block(unsigned index,
				      unsigned nr_entries,
				      value_type v)
{
	array_block<ValueTraits> ab = new_ablock();

	for (unsigned i = 0; i < nr_entries; i++)
		ab.set(i, v);

	block_tree_.insert(index, ab);
}

//----------------------------------------------------------------

template <typename ValueTraits>
ro_array_block<ValueTraits>::ro_array_block(read_ref rr)
	: rr_(rr)
{
}

template <typename ValueTraits>
unsigned
ro_array_block<ValueTraits>::nr_entries() const
{
	array_block_disk const *data =
		reinterpret_cast<array_block_disk const *>(&rr_.data());

	return to_cpu<uint32_t>(data->nr_entries);
}

template <typename ValueTraits>
typename ValueTraits::value_type
ro_array_block<ValueTraits>::get(unsigned index) const
{
        value_type v;
	ValueTraits::unpack(element_at(index), v);
	return v;
}

template <typename ValueTraits>
array_block<ValueTraits>::array_block(write_ref wr)
	: wr_(wr)
{
}

template <typename ValueTraits>
void
array_block<ValueTraits>::set(unsigned index, value_type const &v)
{
	void *elt = element_at(index);
	ValueTraits::pack(v, element_at(index));
}

template <typename ValueTraits>
void
array_block<ValueTraits>::inc_all_entries(typename ValueTraits::ref_counter &rc)
{
	unsigned nr = ro_array_block<ValueTraits>::nr_entries();

	for (unsigned i = 0; i < nr; i++)
		rc.inc(ro_array_block<ValueTraits>::get(i));
}

template <typename  ValueTraits>
void
array_block<ValueTraits>::dec_all_entries(typename ValueTraits::ref_counter &rc)
{
	unsigned nr = ro_array_block<ValueTraits>::nr_entries();

	for (unsigned i = 0; i < nr; i++)
		rc.dec(ro_array_block<ValueTraits>::get(i));
}

//----------------------------------------------------------------

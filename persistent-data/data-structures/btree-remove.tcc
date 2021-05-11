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

namespace persistent_data {
	template <unsigned Levels, typename ValueTraits>
	btree_detail::shadow_child
	btree<Levels, ValueTraits>::
	create_shadow_child(internal_node &parent,
			    unsigned index)
	{
		block_address b = parent.value_at(index);

		pair<write_ref, bool> p = tm_.shadow(b, validator_);
		write_ref &wr = p.first;
		btree_detail::node_type type;

		node_ref<block_traits> n = to_node<block_traits>(wr);
		if (n.get_type() == btree_detail::INTERNAL) {
			type = btree_detail::INTERNAL;
			if (p.second)
				n.inc_children(internal_rc_);
		} else {
			type = btree_detail::LEAF;
			if (p.second) {
				node_ref<ValueTraits> leaf = to_node<ValueTraits>(wr);
				leaf.inc_children(rc_);
			}
		}

		parent.set_value(index, wr.get_location());

		return btree_detail::shadow_child(wr, type);
	}

	template <unsigned Levels, typename ValueTraits>
	void
	btree<Levels, ValueTraits>::
	remove(key const &key)
	{
		using namespace btree_detail;

		block_address block = root_;
		unsigned index = 0;
		shadow_spine spine(tm_, validator_);
		bool need_remove = true;

		for (unsigned level = 0; level < Levels - 1; ++level) {
			need_remove = remove_location<block_traits>(spine, block,
								    key[level], &index,
								    internal_rc_);
			if (!need_remove)
				break;

			internal_node n = spine.get_node<block_traits>();
			block = n.value_at(index);
		}

		if (need_remove) {
			need_remove = remove_location<ValueTraits>(spine, block,
								   key[Levels - 1], &index,
								   rc_);
			if (need_remove) {
				leaf_node leaf = spine.get_node<ValueTraits>();
				leaf.delete_at(index);
			}
		}

		root_ = spine.get_root();
	}

	template <unsigned Levels, typename _>
	template <typename ValueTraits, typename RC>
	bool
	btree<Levels, _>::
	remove_location(btree_detail::shadow_spine &spine,
			block_address block,
			uint64_t key,
			unsigned *index,
			RC &leaf_rc)
	{
		using namespace btree_detail;

		unsigned i = *index;

		for (;;) {
			bool inc = spine.step(block);
			if (inc)
				inc_children<ValueTraits>(spine, leaf_rc);

			// patch up the parent to point to the new shadow
			if (spine.has_parent()) {
				internal_node p = spine.get_parent();
				p.set_value(i, spine.get_block());
			}

			internal_node n = spine.get_node<block_traits>();
			if (n.get_type() == btree_detail::LEAF) {
				node_ref<ValueTraits> leaf = spine.get_node<ValueTraits>();
				boost::optional<unsigned> idx = leaf.exact_search(key);
				if (!idx)
					return false;
				*index = *idx;
				return true;
			}

			bool r = rebalance_children<ValueTraits>(spine, key);
			if (!r)
				return false;

			n = spine.get_node<block_traits>();
			if (n.get_type() == btree_detail::LEAF) {
				node_ref<ValueTraits> leaf = spine.get_node<ValueTraits>();
				boost::optional<unsigned> idx = leaf.exact_search(key);
				if (!idx)
					return false;
				*index = *idx;
				return true;
			}

			i = n.lower_bound(key);
			block = n.value_at(i);
		}

		return true;
	}

	template <unsigned Levels, typename _>
	template <typename ValueTraits>
	bool
	btree<Levels, _>::
	rebalance_children(btree_detail::shadow_spine &spine, uint64_t key)
	{
		internal_node n = spine.get_node<block_traits>();

		// compact the path if there's only one child
		if (n.get_nr_entries() == 1) {
			block_address b = n.value_at(0);
			read_ref child = tm_.read_lock(b, validator_);

			::memcpy(n.raw(), child.data(), read_ref::BLOCK_SIZE);

			tm_.get_sm()->dec(child.get_location());
			return true;
		}

		int i = n.lower_bound(key);
		if (i < 0)
			return false;

		bool has_left_sibling = i > 0;
		bool has_right_sibling = static_cast<unsigned>(i) < (n.get_nr_entries() - 1);

		if (!has_left_sibling)
			rebalance2<ValueTraits>(spine, i);
		else if (!has_right_sibling)
			rebalance2<ValueTraits>(spine, i - 1);
		else
			rebalance3<ValueTraits>(spine, i - 1);

		return true;
	}

	template <unsigned Levels, typename _>
	template <typename ValueTraits>
	void
	btree<Levels, _>::
	rebalance2(btree_detail::shadow_spine &spine, unsigned left_index)
	{
		internal_node parent = spine.get_node<block_traits>();
		shadow_child left = create_shadow_child(parent, left_index);
		shadow_child right = create_shadow_child(parent, left_index + 1);

		// FIXME: ugly
		if (left.get_type() == btree_detail::INTERNAL) {
			internal_node l = left.get_node<block_traits>();
			internal_node r = right.get_node<block_traits>();
			__rebalance2(parent, l, r, left_index);
		} else {
			node_ref<ValueTraits> l = left.get_node<ValueTraits>();
			node_ref<ValueTraits> r = right.get_node<ValueTraits>();
			__rebalance2(parent, l, r, left_index);
		}
	}

	template <unsigned Levels, typename _>
	template <typename ValueTraits>
	void
	btree<Levels, _>::
	__rebalance2(internal_node &parent,
		     node_ref<ValueTraits> &left,
		     node_ref<ValueTraits> &right,
		     unsigned left_index)
	{
		unsigned nr_left = left.get_nr_entries();
		unsigned nr_right = right.get_nr_entries();
		unsigned right_index = left_index + 1;

		unsigned threshold = 2 * (left.merge_threshold() + 1);
		if (nr_left + nr_right < threshold) {
			// Merge the right child into the left
			left.copy_entries_to_left(right, nr_right);
			left.set_nr_entries(nr_left + nr_right);
			parent.delete_at(right_index);
			tm_.get_sm()->dec(right.get_location());
		} else {
			// Rebalance
			unsigned target_left = (nr_left + nr_right) / 2;
			left.move_entries(right, nr_left - target_left);
			parent.set_key(right_index, right.key_at(0));
		}
	}

	template <unsigned Levels, typename _>
	template <typename ValueTraits>
	void
	btree<Levels, _>::
	rebalance3(btree_detail::shadow_spine &spine, unsigned left_index)
	{
		internal_node parent = spine.get_node<block_traits>();
		shadow_child left = create_shadow_child(parent, left_index);
		shadow_child center = create_shadow_child(parent, left_index + 1);
		shadow_child right = create_shadow_child(parent, left_index + 2);

		// FIXME: ugly
		if (left.get_type() == btree_detail::INTERNAL) {
			internal_node l = left.get_node<block_traits>();
			internal_node c = center.get_node<block_traits>();
			internal_node r = right.get_node<block_traits>();
			__rebalance3(parent, l, c, r, left_index);
		} else {
			node_ref<ValueTraits> l = left.get_node<ValueTraits>();
			node_ref<ValueTraits> c = center.get_node<ValueTraits>();
			node_ref<ValueTraits> r = right.get_node<ValueTraits>();
			__rebalance3(parent, l, c, r, left_index);
		}
	}

	template <unsigned Levels, typename _>
	template <typename ValueTraits>
	void
	btree<Levels, _>::
	__rebalance3(internal_node &parent,
		     node_ref<ValueTraits> &left,
		     node_ref<ValueTraits> &center,
		     node_ref<ValueTraits> &right,
		     unsigned left_index)
	{
		unsigned nr_left = left.get_nr_entries();
		unsigned nr_center = center.get_nr_entries();
		unsigned nr_right = right.get_nr_entries();

		unsigned threshold = left.merge_threshold() * 4 + 1;

		if ((nr_left + nr_center + nr_right) < threshold)
			delete_center_node(parent, left, center, right, left_index);
		else
			redistribute3(parent, left, center, right, left_index);
	}

	template <unsigned Levels, typename _>
	template <typename ValueTraits>
	void
	btree<Levels, _>::
	delete_center_node(internal_node &parent,
			   node_ref<ValueTraits> &left,
			   node_ref<ValueTraits> &center,
			   node_ref<ValueTraits> &right,
			   unsigned left_index)
	{
		unsigned center_index = left_index + 1;
		unsigned right_index = left_index + 2;

		unsigned max_entries = left.get_max_entries();
		unsigned nr_left = left.get_nr_entries();
		unsigned nr_center = center.get_nr_entries();
		unsigned nr_right = right.get_nr_entries();
		unsigned shift = std::min(max_entries - nr_left, nr_center);

		if (nr_left + shift > max_entries)
			throw std::runtime_error("too many entries");

		left.copy_entries_to_left(center, shift);
		left.set_nr_entries(nr_left + shift);

		if (shift != nr_center) {
			shift = nr_center - shift;
			if ((nr_right + shift) > max_entries)
				throw std::runtime_error("too many entries");
			right.shift_entries_right(shift);
			center.copy_entries_to_right(right, shift);
			right.set_nr_entries(nr_right + shift);
		}
		parent.set_key(right_index, right.key_at(0));

		parent.delete_at(center_index);
		--right_index;

		tm_.get_sm()->dec(center.get_location());
		__rebalance2(parent, left, right, left_index);
	}

	template <unsigned Levels, typename _>
	template <typename ValueTraits>
	void
	btree<Levels, _>::
	redistribute3(internal_node &parent,
		      node_ref<ValueTraits> &left,
		      node_ref<ValueTraits> &center,
		      node_ref<ValueTraits> &right,
		      unsigned left_index)
	{
		unsigned center_index = left_index + 1;
		unsigned right_index = left_index + 2;

		unsigned nr_left = left.get_nr_entries();
		unsigned nr_center = center.get_nr_entries();
		unsigned nr_right = right.get_nr_entries();

		unsigned max_entries = left.get_max_entries();
		unsigned total = nr_left + nr_center + nr_right;
		unsigned target_right = total / 3;
		unsigned remainder = (target_right * 3) != total;
		unsigned target_left = target_right + remainder;

		if (target_left > max_entries || target_right > max_entries)
			throw std::runtime_error("too many entries");

		if (nr_left < nr_right) {
			int s = nr_left - target_left;

			if (s < 0 && nr_center < static_cast<unsigned>(-s)) {
				// not enough in central node
				left.move_entries(center, -nr_center);
				s += nr_center;
				left.move_entries(right, s);
				nr_right += s;
			} else
				left.move_entries(center, s);

			center.move_entries(right, target_right - nr_right);

		} else {
			int s = target_right - nr_right;

			if (s > 0 && nr_center < static_cast<unsigned>(s)) {
				// not enough in central node
				center.move_entries(right, nr_center);
				s -= nr_center;
				left.move_entries(right, s);
				nr_left -= s;
			} else
				center.move_entries(right, s);

			left.move_entries(center, nr_left - target_left);
		}

		parent.set_key(center_index, center.key_at(0));
		parent.set_key(right_index, right.key_at(0));
	}
};

#include "space_map_disk.h"

#include "endian_utils.h"
#include "math_utils.h"
#include "space_map_disk_structures.h"
#include "transaction_manager.h"

using namespace boost;
using namespace persistent_data;
using namespace std;
using namespace sm_disk_detail;


//----------------------------------------------------------------

namespace {
	class bitmap {
	public:
		typedef transaction_manager::read_ref read_ref;
		typedef transaction_manager::write_ref write_ref;

		bitmap(transaction_manager::ptr tm,
		       index_entry const &ie)
			: tm_(tm),
			  ie_(ie) {
		}

		ref_t lookup(unsigned b) const {
			read_ref rr = tm_->read_lock(ie_.blocknr_);
			void const *bits = bitmap_data(rr);
			ref_t b1 = test_bit_le(bits, b * 2);
			ref_t b2 = test_bit_le(bits, b * 2 + 1);
			ref_t result = b2 ? 1 : 0;
			result |= b1 ? 0b10 : 0;
			return result;
		}

		void insert(unsigned b, ref_t n) {
			write_ref wr = tm_->shadow(ie_.blocknr_).first;
			void *bits = bitmap_data(wr);
			bool was_free = !test_bit_le(bits, b * 2) && !test_bit_le(bits, b * 2 + 1);
			if (n == 1 || n == 3)
				set_bit_le(bits, b * 2 + 1);
			else
				clear_bit_le(bits, b * 2 + 1);

			if (n == 2 || n == 3)
				set_bit_le(bits, b * 2);
			else
				clear_bit_le(bits, b * 2);

			ie_.blocknr_ = wr.get_location();

			if (was_free && n > 0) {
				ie_.nr_free_--;
				if (b == ie_.none_free_before_)
					ie_.none_free_before_++;
			}

			if (!was_free && n == 0) {
				ie_.nr_free_++;
				if (b < ie_.none_free_before_)
					ie_.none_free_before_ = b;
			}
		}

		unsigned find_free(unsigned end) {
			for (unsigned i = ie_.none_free_before_; i < end; i++) {
				if (lookup(i) == 0) {
					insert(i, 1);
					ie_.none_free_before_ = i + 1;
					return i;
				}
			}

			throw std::runtime_error("no free entry in bitmap");
		}

		index_entry const &get_ie() const {
			return ie_;
		}

	private:
		void *bitmap_data(transaction_manager::write_ref &wr) {
			bitmap_header *h = reinterpret_cast<bitmap_header *>(&wr.data()[0]);
			return h + 1;
		}

		void const *bitmap_data(transaction_manager::read_ref &rr) const {
			bitmap_header const *h = reinterpret_cast<bitmap_header const *>(&rr.data()[0]);
			return h + 1;
		}

		transaction_manager::ptr tm_;
		index_entry ie_;
	};

	struct ref_count_traits {
		typedef __le32 disk_type;
		typedef uint32_t value_type;
		typedef NoOpRefCounter<uint32_t> ref_counter;

		static void unpack(disk_type const &d, value_type &v) {
			v = to_cpu<value_type>(d);
		}

		static void pack(value_type const &v, disk_type &d) {
			d = to_disk<disk_type>(v);
		}
	};

	class ref_count_checker : public btree_checker<1, ref_count_traits> {
	public:
		typedef boost::shared_ptr<ref_count_checker> ptr;

		ref_count_checker(block_counter &counter)
			: btree_checker<1, ref_count_traits>(counter) {
		}
	};

	class sm_disk_base : public checked_space_map {
	public:
		typedef boost::shared_ptr<sm_disk_base> ptr;
		typedef transaction_manager::read_ref read_ref;
		typedef transaction_manager::write_ref write_ref;

		sm_disk_base(transaction_manager::ptr tm)
			: tm_(tm),
			  entries_per_block_((MD_BLOCK_SIZE - sizeof(bitmap_header)) * 4),
			  nr_blocks_(0),
			  nr_allocated_(0),
			  ref_counts_(tm_, ref_count_traits::ref_counter()) {
		}

		sm_disk_base(transaction_manager::ptr tm,
			     sm_root const &root)
			: tm_(tm),
			  entries_per_block_((MD_BLOCK_SIZE - sizeof(bitmap_header)) * 4),
			  nr_blocks_(root.nr_blocks_),
			  nr_allocated_(root.nr_allocated_),
			  ref_counts_(tm_, root.ref_count_root_, ref_count_traits::ref_counter()) {
		}

		block_address get_nr_blocks() const {
			return nr_blocks_;
		}

		block_address get_nr_free() const {
			return nr_blocks_ - nr_allocated_;
		}

		ref_t get_count(block_address b) const {
			ref_t count = lookup_bitmap(b);
			if (count == 3)
				return lookup_ref_count(b);

			return count;
		}

		void set_count(block_address b, ref_t c) {
			ref_t old = get_count(b);

			if (c == old)
				return;

			if (c > 2) {
				if (old < 3)
					insert_bitmap(b, 3);
				insert_ref_count(b, c);
			} else {
				if (old > 2)
					remove_ref_count(b);
				insert_bitmap(b, c);
			}

			if (old == 0)
				nr_allocated_++;
			else if (c == 0)
				nr_allocated_--;
		}

		void commit() {
			commit_ies();
		}

		void inc(block_address b) {
			// FIXME: 2 get_counts
			ref_t old = get_count(b);
			set_count(b, old + 1);
		}

		void dec(block_address b) {
			ref_t old = get_count(b);
			set_count(b, old - 1);
		}

		block_address new_block() {
			// silly to always start searching from the
			// beginning.
			block_address nr_indexes = div_up<block_address>(nr_blocks_, entries_per_block_);
			for (block_address index = 0; index < nr_indexes; index++) {
				index_entry ie = find_ie(index);

				bitmap bm(tm_, ie);
				block_address b = bm.find_free((index == nr_indexes - 1) ?
							       nr_blocks_ % entries_per_block_ : entries_per_block_);
				save_ie(b, bm.get_ie());
				nr_allocated_++;
				b = (index * entries_per_block_) + b;
				assert(get_count(b) == 1);
				return b;
			}

			throw runtime_error("out of space");
		}

		bool count_possibly_greater_than_one(block_address b) const {
			return get_count(b) > 1;
		}

		virtual void extend(block_address extra_blocks) {
			block_address nr_blocks = nr_blocks_ + extra_blocks;

			block_address bitmap_count = div_up<block_address>(nr_blocks, entries_per_block_);
			block_address old_bitmap_count = div_up<block_address>(nr_blocks_, entries_per_block_);
			for (block_address i = old_bitmap_count; i < bitmap_count; i++) {
				write_ref wr = tm_->new_block();

				struct index_entry ie;
				ie.blocknr_ = wr.get_location();
				ie.nr_free_ = i == (bitmap_count - 1) ?
					(nr_blocks % entries_per_block_) : entries_per_block_;
				ie.none_free_before_ = 0;

				save_ie(i, ie);
			}

			nr_blocks_ = nr_blocks;
		}

		virtual void check(block_counter &counter) const {
			ref_count_checker::ptr v(new ref_count_checker(counter));
			ref_counts_.visit(v);
		}

	protected:
		transaction_manager::ptr get_tm() const {
			return tm_;
		}

		block_address get_nr_allocated() const {
			return nr_allocated_;
		}

		block_address get_ref_count_root() const {
			return ref_counts_.get_root();
		}

		unsigned get_entries_per_block() const {
			return entries_per_block_;
		}

	private:
		virtual index_entry find_ie(block_address b) const = 0;
		virtual void save_ie(block_address b, struct index_entry ie) = 0;
		virtual void commit_ies() = 0;

		ref_t lookup_bitmap(block_address b) const {
			index_entry ie = find_ie(b / entries_per_block_);
			bitmap bm(tm_, ie);
			return bm.lookup(b % entries_per_block_);
		}

		void insert_bitmap(block_address b, unsigned n) {
			if (n > 3)
				throw runtime_error("bitmap can only hold 2 bit values");

			index_entry ie = find_ie(b / entries_per_block_);
			bitmap bm(tm_, ie);
			bm.insert(b % entries_per_block_, n);
			save_ie(b, bm.get_ie());
		}

		ref_t lookup_ref_count(block_address b) const {
			uint64_t key[1] = {b};
			optional<ref_t> mvalue = ref_counts_.lookup(key);
			if (!mvalue)
				throw runtime_error("ref count not in tree");
			return *mvalue;
		}

		void insert_ref_count(block_address b, ref_t count) {
			uint64_t key[1] = {b};
			ref_counts_.insert(key, count);
		}

		void remove_ref_count(block_address b) {
			uint64_t key[1] = {b};
			ref_counts_.remove(key);
		}

		transaction_manager::ptr tm_;
		uint32_t entries_per_block_;
		block_address nr_blocks_;
		block_address nr_allocated_;

		btree<1, ref_count_traits> ref_counts_;
	};

	class bitmap_tree_validator : public btree_checker<1, index_entry_traits> {
	public:
		typedef boost::shared_ptr<bitmap_tree_validator> ptr;

		bitmap_tree_validator(block_counter &counter)
			: btree_checker<1, index_entry_traits>(counter) {
		}

		bool visit_leaf(unsigned level,
				optional<uint64_t> key,
				btree_detail::node_ref<index_entry_traits> const &n) {
			bool r = btree_checker<1, index_entry_traits>::visit_leaf(level, key, n);
			if (!r)
				return r;

			for (unsigned i = 0; i < n.get_nr_entries(); i++) {
				if (seen_indexes_.count(n.key_at(i)) > 0) {
					ostringstream out;
					out << "index entry " << i << " is present twice";
					throw runtime_error(out.str());
				}

				seen_indexes_.insert(n.key_at(i));
				btree_checker<1, index_entry_traits>::get_counter().inc(n.value_at(i).blocknr_);
			}

			return true;
		}

		void check_all_index_entries_present(block_address nr_entries) {
			for (block_address i = 0; i < nr_entries; i++) {
				if (seen_indexes_.count(i) == 0) {
					ostringstream out;
					out << "missing index entry " << i;
					throw runtime_error(out.str());
				}
			}

			set<block_address>::const_iterator it;
			for (it = seen_indexes_.begin(); it != seen_indexes_.end(); ++it) {
				if (*it >= nr_entries) {
					ostringstream out;
					out << "unexpected index entry " << *it;
					throw runtime_error(out.str());
				}
			}
		}

	private:
		set<block_address> seen_indexes_;
	};

	class sm_disk : public sm_disk_base {
	public:
		typedef boost::shared_ptr<sm_disk> ptr;

		sm_disk(transaction_manager::ptr tm)
			: sm_disk_base(tm),
			  bitmaps_(sm_disk_base::get_tm(), index_entry_traits::ref_counter()) {
		}

		sm_disk(transaction_manager::ptr tm,
			sm_root const &root)
			: sm_disk_base(tm, root),
			  bitmaps_(sm_disk_base::get_tm(), root.bitmap_root_, index_entry_traits::ref_counter()) {
		}

		size_t root_size() {
			return sizeof(sm_root_disk);
		}

		void copy_root(void *dest, size_t len) {
			sm_root_disk d;
			sm_root v;

			if (len < sizeof(d))
				throw runtime_error("root too small");

			v.nr_blocks_ = sm_disk_base::get_nr_blocks();
			v.nr_allocated_ = sm_disk_base::get_nr_allocated();
			v.bitmap_root_ = bitmaps_.get_root();
			v.ref_count_root_ = sm_disk_base::get_ref_count_root();
			sm_root_traits::pack(v, d);
			::memcpy(dest, &d, sizeof(d));
		}

		void check(block_counter &counter) const {
			sm_disk_base::check(counter);

			bitmap_tree_validator::ptr v(new bitmap_tree_validator(counter));
			bitmaps_.visit(v);

			block_address nr_entries = div_up<block_address>(get_nr_blocks(), get_entries_per_block());
			v->check_all_index_entries_present(nr_entries);
		}

	private:
		index_entry find_ie(block_address ie_index) const {
			uint64_t key[1] = {ie_index};
			optional<index_entry> mindex = bitmaps_.lookup(key);
			if (!mindex)
				throw runtime_error("Couldn't lookup bitmap");

			return *mindex;
		}

		void save_ie(block_address ie_index, struct index_entry ie) {
			uint64_t key[1] = {ie_index};
			bitmaps_.insert(key, ie);
		}

		void commit_ies() {
		}

		btree<1, index_entry_traits> bitmaps_;
	};

	class sm_metadata : public sm_disk_base {
	public:
		typedef boost::shared_ptr<sm_metadata> ptr;

		sm_metadata(transaction_manager::ptr tm)
			: sm_disk_base(tm),
			  entries_(MAX_METADATA_BITMAPS) {
			// FIXME: allocate a new bitmap root
		}

		sm_metadata(transaction_manager::ptr tm,
			    sm_root const &root)
			: sm_disk_base(tm, root),
			  bitmap_root_(root.bitmap_root_),
			  entries_(MAX_METADATA_BITMAPS) {
			load_ies();
		}

		size_t root_size() {
			return sizeof(sm_root_disk);
		}

		// FIXME: common code
		void copy_root(void *dest, size_t len) {
			sm_root_disk d;
			sm_root v;

			if (len < sizeof(d))
				throw runtime_error("root too small");

			v.nr_blocks_ = sm_disk_base::get_nr_blocks();
			v.nr_allocated_ = sm_disk_base::get_nr_allocated();
			v.bitmap_root_ = bitmap_root_;
			v.ref_count_root_ = sm_disk_base::get_ref_count_root();
			sm_root_traits::pack(v, d);
			::memcpy(dest, &d, sizeof(d));
		}

		void check(block_counter &counter) const {
			sm_disk_base::check(counter);

			counter.inc(bitmap_root_);
			for (unsigned i = 0; i < entries_.size(); i++)
				if (entries_[i].blocknr_ != 0) // superblock
					counter.inc(entries_[i].blocknr_);
		}

	private:
		index_entry find_ie(block_address ie_index) const {
			return entries_[ie_index];
		}

		void save_ie(block_address ie_index, struct index_entry ie) {
			entries_[ie_index] = ie;
		}

		void load_ies() {
			block_manager<>::read_ref rr =
				sm_disk_base::get_tm()->read_lock(bitmap_root_);

			metadata_index const *mdi = reinterpret_cast<metadata_index const *>(&rr.data());

			unsigned nr_indexes = div_up<block_address>(sm_disk_base::get_nr_blocks(),
								    sm_disk_base::get_entries_per_block());
			for (unsigned i = 0; i < nr_indexes; i++)
				index_entry_traits::unpack(*(mdi->index + i), entries_[i]);
		}

		void commit_ies() {
			std::pair<block_manager<>::write_ref, bool> p =
				sm_disk_base::get_tm()->shadow(bitmap_root_);

			bitmap_root_ = p.first.get_location();
			metadata_index *mdi = reinterpret_cast<metadata_index *>(&p.first.data());

			mdi->csum_ = to_disk<__le32, uint32_t>(0);
			mdi->padding_ = to_disk<__le32, uint32_t>(0);
			mdi->blocknr_ = to_disk<__le64>(bitmap_root_);

			for (unsigned i = 0; i < entries_.size(); i++)
				index_entry_traits::pack(entries_[i], mdi->index[i]);
		}

		block_address bitmap_root_;
		std::vector<index_entry> entries_;
	};
}

//----------------------------------------------------------------

checked_space_map::ptr
persistent_data::create_disk_sm(transaction_manager::ptr tm,
				block_address nr_blocks)
{
	checked_space_map::ptr sm(new sm_disk(tm));
	sm->extend(nr_blocks);
	return sm;
}

checked_space_map::ptr
persistent_data::open_disk_sm(transaction_manager::ptr tm, void *root)
{
	sm_root_disk d;
	sm_root v;

	::memcpy(&d, root, sizeof(d));
	sm_root_traits::unpack(d, v);
	return checked_space_map::ptr(new sm_disk(tm, v));
}

checked_space_map::ptr
persistent_data::open_metadata_sm(transaction_manager::ptr tm, void * root)
{
	sm_root_disk d;
	sm_root v;

	::memcpy(&d, root, sizeof(d));
	sm_root_traits::unpack(d, v);
	return checked_space_map::ptr(new sm_metadata(tm, v));
}

//----------------------------------------------------------------

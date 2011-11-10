#include "space_map_disk.h"

#include "checksum.h"
#include "endian_utils.h"
#include "math_utils.h"
#include "space_map_disk_structures.h"
#include "space_map_recursive.h"
#include "transaction_manager.h"

using namespace boost;
using namespace persistent_data;
using namespace std;
using namespace sm_disk_detail;


//----------------------------------------------------------------

namespace {
	uint64_t const BITMAP_CSUM_XOR = 240779;

	struct bitmap_block_validator : public block_manager<>::validator {
		virtual void check(block_manager<>::const_buffer &b, block_address location) const {
			bitmap_header const *data = reinterpret_cast<bitmap_header const *>(&b);
			crc32c sum(BITMAP_CSUM_XOR);
			sum.append(&data->not_used, MD_BLOCK_SIZE - sizeof(uint32_t));
			if (sum.get_sum() != to_cpu<uint32_t>(data->csum))
				throw runtime_error("bad checksum in space map bitmap");

			if (to_cpu<uint64_t>(data->blocknr) != location)
				throw runtime_error("bad block nr in space map bitmap");
		}

		virtual void prepare(block_manager<>::buffer &b, block_address location) const {
			bitmap_header *data = reinterpret_cast<bitmap_header *>(&b);
			data->blocknr = to_disk<base::__le64, uint64_t>(location);

			crc32c sum(BITMAP_CSUM_XOR);
			sum.append(&data->not_used, MD_BLOCK_SIZE - sizeof(uint32_t));
			data->csum = to_disk<base::__le32>(sum.get_sum());
		}
	};

	block_manager<>::validator::ptr
	bitmap_validator() {
		return block_manager<>::validator::ptr(new bitmap_block_validator());
	}

	//--------------------------------

	uint64_t const INDEX_CSUM_XOR = 160478;

	// FIXME: factor out the common code in these validators
	struct index_block_validator : public block_manager<>::validator {
		virtual void check(block_manager<>::const_buffer &b, block_address location) const {
			metadata_index const *mi = reinterpret_cast<metadata_index const *>(&b);
			crc32c sum(INDEX_CSUM_XOR);
			sum.append(&mi->padding_, MD_BLOCK_SIZE - sizeof(uint32_t));
			if (sum.get_sum() != to_cpu<uint32_t>(mi->csum_))
				throw runtime_error("bad checksum in metadata index block");

			if (to_cpu<uint64_t>(mi->blocknr_) != location)
				throw runtime_error("bad block nr in metadata index block");
		}

		virtual void prepare(block_manager<>::buffer &b, block_address location) const {
			metadata_index *mi = reinterpret_cast<metadata_index *>(&b);
			mi->blocknr_ = to_disk<base::__le64, uint64_t>(location);

			crc32c sum(INDEX_CSUM_XOR);
			sum.append(&mi->padding_, MD_BLOCK_SIZE - sizeof(uint32_t));
			mi->csum_ = to_disk<base::__le32>(sum.get_sum());
		}
	};


	block_manager<>::validator::ptr
	index_validator() {
		return block_manager<>::validator::ptr(new index_block_validator());
	}

	//--------------------------------

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
			read_ref rr = tm_->read_lock(ie_.blocknr_, bitmap_validator());
			void const *bits = bitmap_data(rr);
			ref_t b1 = test_bit_le(bits, b * 2);
			ref_t b2 = test_bit_le(bits, b * 2 + 1);
			ref_t result = b2 ? 1 : 0;
			result |= b1 ? 0b10 : 0;
			return result;
		}

		void insert(unsigned b, ref_t n) {
			write_ref wr = tm_->shadow(ie_.blocknr_, bitmap_validator()).first;
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

		boost::optional<unsigned> find_free(unsigned begin, unsigned end) {
			for (unsigned i = max(begin, ie_.none_free_before_); i < end; i++) {
				if (lookup(i) == 0) {
					insert(i, 1);
					ie_.none_free_before_ = i + 1;
					return boost::optional<unsigned>(i);
				}
			}

			return boost::optional<unsigned>();
		}

		index_entry const &get_ie() const {
			return ie_;
		}

		void iterate(block_address offset, block_address hi, space_map::iterator &it) const {
			read_ref rr = tm_->read_lock(ie_.blocknr_, bitmap_validator());
			void const *bits = bitmap_data(rr);

			for (unsigned b = 0; b < hi; b++) {
				ref_t b1 = test_bit_le(bits, b * 2);
				ref_t b2 = test_bit_le(bits, b * 2 + 1);
				ref_t result = b2 ? 1 : 0;
				result |= b1 ? 0b10 : 0;
				it(offset + b, result);
			}
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

	class index_store {
	public:
		typedef boost::shared_ptr<index_store> ptr;

		virtual void resize(block_address nr_indexes) = 0;
		virtual index_entry find_ie(block_address b) const = 0;
		virtual void save_ie(block_address b, struct index_entry ie) = 0;
		virtual void commit_ies() = 0;
		virtual ptr clone() const = 0;
		virtual block_address get_root() const = 0;
		virtual void check(block_counter &counter, block_address nr_index_entries) const = 0;
	};

	unsigned const ENTRIES_PER_BLOCK = (MD_BLOCK_SIZE - sizeof(bitmap_header)) * 4;

	class sm_disk_base : public checked_space_map {
	public:
		typedef boost::shared_ptr<sm_disk_base> ptr;
		typedef transaction_manager::read_ref read_ref;
		typedef transaction_manager::write_ref write_ref;

		sm_disk_base(index_store::ptr indexes,
			     transaction_manager::ptr tm)
			: tm_(tm),
			  indexes_(indexes),
			  nr_blocks_(0),
			  nr_allocated_(0),
			  ref_counts_(tm_, ref_count_traits::ref_counter()) {
		}

		sm_disk_base(index_store::ptr indexes,
			     transaction_manager::ptr tm,
			     sm_root const &root)
			: tm_(tm),
			  indexes_(indexes),
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
			// FIXME: silly to always start searching from the
			// beginning.
			block_address nr_indexes = div_up<block_address>(nr_blocks_, ENTRIES_PER_BLOCK);
			for (block_address index = 0; index < nr_indexes; index++) {
				index_entry ie = find_ie(index);

				bitmap bm(tm_, ie);
				optional<unsigned> maybe_b = bm.find_free(0, (index == nr_indexes - 1) ?
							  nr_blocks_ % ENTRIES_PER_BLOCK : ENTRIES_PER_BLOCK);
				if (maybe_b) {
					block_address b = *maybe_b;
					save_ie(index, bm.get_ie());
					nr_allocated_++;
					b = (index * ENTRIES_PER_BLOCK) + b;
					assert(get_count(b) == 1);
					return b;
				}
			}

			throw runtime_error("out of space");
		}

		bool count_possibly_greater_than_one(block_address b) const {
			return get_count(b) > 1;
		}

		virtual void extend(block_address extra_blocks) {
			block_address nr_blocks = nr_blocks_ + extra_blocks;

			block_address bitmap_count = div_up<block_address>(nr_blocks, ENTRIES_PER_BLOCK);
			block_address old_bitmap_count = div_up<block_address>(nr_blocks_, ENTRIES_PER_BLOCK);

			indexes_->resize(bitmap_count);
			for (block_address i = old_bitmap_count; i < bitmap_count; i++) {
				write_ref wr = tm_->new_block(bitmap_validator());

				index_entry ie;
				ie.blocknr_ = wr.get_location();
				ie.nr_free_ = i == (bitmap_count - 1) ?
					(nr_blocks % ENTRIES_PER_BLOCK) : ENTRIES_PER_BLOCK;
				ie.none_free_before_ = 0;

				save_ie(i, ie);
			}

			nr_blocks_ = nr_blocks;
		}

		virtual void check(block_counter &counter) const {
			ref_count_checker::ptr v(new ref_count_checker(counter));
			ref_counts_.visit(v);

			block_address nr_entries = div_up<block_address>(get_nr_blocks(), ENTRIES_PER_BLOCK);
			indexes_->check(counter, nr_entries);
		}

		struct look_aside_iterator : public iterator {
			look_aside_iterator(sm_disk_base const &smd, iterator &it)
				: smd_(smd),
				  it_(it) {
			}

			virtual void operator () (block_address b, ref_t c) {
				it_(b, c == 3 ? smd_.lookup_ref_count(b) : c);
			}

			sm_disk_base const &smd_;
			iterator &it_;
		};

		friend struct look_aside_iterator;

		virtual void iterate(iterator &it) const {
			look_aside_iterator wrapper(*this, it);
			unsigned nr_indexes = div_up<block_address>(nr_blocks_, ENTRIES_PER_BLOCK);

			for (unsigned i = 0; i < nr_indexes; i++) {
				unsigned hi = (i == nr_indexes - 1) ? (nr_blocks_ % ENTRIES_PER_BLOCK) : ENTRIES_PER_BLOCK;
				index_entry ie = find_ie(i);
				bitmap bm(tm_, ie);
				bm.iterate(i * ENTRIES_PER_BLOCK, hi, wrapper);
			}
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
			v.bitmap_root_ = get_index_store()->get_root();
			v.ref_count_root_ = sm_disk_base::get_ref_count_root();

			sm_root_traits::pack(v, d);
			::memcpy(dest, &d, sizeof(d));
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

		index_store::ptr get_index_store() const {
			return indexes_;
		}

	private:
		// FIXME: remove these, they're just for the transistion
		index_entry find_ie(block_address b) const {
			return indexes_->find_ie(b);
		}

		void save_ie(block_address b, struct index_entry ie) {
			return indexes_->save_ie(b, ie);
		}

		void commit_ies() {
			return indexes_->commit_ies();
		}

		ref_t lookup_bitmap(block_address b) const {
			index_entry ie = find_ie(b / ENTRIES_PER_BLOCK);
			bitmap bm(tm_, ie);
			return bm.lookup(b % ENTRIES_PER_BLOCK);
		}

		void insert_bitmap(block_address b, unsigned n) {
			if (n > 3)
				throw runtime_error("bitmap can only hold 2 bit values");

			index_entry ie = find_ie(b / ENTRIES_PER_BLOCK);
			bitmap bm(tm_, ie);
			bm.insert(b % ENTRIES_PER_BLOCK, n);
			save_ie(b / ENTRIES_PER_BLOCK, bm.get_ie());
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
		index_store::ptr indexes_;
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
				bool sub_root,
				optional<uint64_t> key,
				btree_detail::node_ref<index_entry_traits> const &n) {
			bool r = btree_checker<1, index_entry_traits>::visit_leaf(level, sub_root, key, n);
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

	class btree_index_store : public index_store {
	public:
		typedef boost::shared_ptr<btree_index_store> ptr;

		btree_index_store(transaction_manager::ptr tm)
			: tm_(tm),
			  bitmaps_(tm, index_entry_traits::ref_counter()) {
		}

		btree_index_store(transaction_manager::ptr tm,
				  block_address root)
			: tm_(tm),
			  bitmaps_(tm, root, index_entry_traits::ref_counter()) {
		}

		virtual void resize(block_address nr_entries) {
			// No op
		}

		virtual index_entry find_ie(block_address ie_index) const {
			uint64_t key[1] = {ie_index};
			optional<index_entry> mindex = bitmaps_.lookup(key);
			if (!mindex)
				throw runtime_error("Couldn't lookup bitmap");

			return *mindex;
		}

		virtual void save_ie(block_address ie_index, struct index_entry ie) {
			uint64_t key[1] = {ie_index};
			bitmaps_.insert(key, ie);
		}

		virtual void commit_ies() {
			// No op
		}

		virtual index_store::ptr clone() const {
			return index_store::ptr(new btree_index_store(tm_, bitmaps_.get_root()));
		}

		virtual block_address get_root() const {
			return bitmaps_.get_root();
		}

		virtual void check(block_counter &counter, block_address nr_index_entries) const {
			bitmap_tree_validator::ptr v(new bitmap_tree_validator(counter));
			bitmaps_.visit(v);
			v->check_all_index_entries_present(nr_index_entries);
		}

	private:
		transaction_manager::ptr tm_;
		btree<1, index_entry_traits> bitmaps_;
	};

	class sm_disk : public sm_disk_base {
	public:
		typedef boost::shared_ptr<sm_disk> ptr;

		sm_disk(transaction_manager::ptr tm)
			: sm_disk_base(index_store::ptr(new btree_index_store(tm)), tm) {
		}

		sm_disk(transaction_manager::ptr tm,
			sm_root const &root)
			: sm_disk_base(index_store::ptr(new btree_index_store(tm, root.bitmap_root_)), tm, root) {
		}
	};

	class metadata_index_store : public index_store {
	public:
		typedef boost::shared_ptr<metadata_index_store> ptr;

		metadata_index_store(transaction_manager::ptr tm)
			: tm_(tm) {
			block_manager<>::write_ref wr = tm_->new_block(index_validator());
			bitmap_root_ = wr.get_location();
		}

		metadata_index_store(transaction_manager::ptr tm, block_address root, block_address nr_indexes)
			: tm_(tm),
			  bitmap_root_(root) {
			resize(nr_indexes);
			load_ies();
		}

		virtual void resize(block_address nr_indexes) {
			entries_.resize(nr_indexes);
		}

		virtual index_entry find_ie(block_address ie_index) const {
			return entries_[ie_index];
		}

		virtual void save_ie(block_address ie_index, struct index_entry ie) {
			entries_[ie_index] = ie;
		}

		virtual void commit_ies() {
			std::pair<block_manager<>::write_ref, bool> p =
				tm_->shadow(bitmap_root_, index_validator());

			bitmap_root_ = p.first.get_location();
			metadata_index *mdi = reinterpret_cast<metadata_index *>(&p.first.data());

			for (unsigned i = 0; i < entries_.size(); i++)
				index_entry_traits::pack(entries_[i], mdi->index[i]);
		}

		virtual index_store::ptr clone() const {
			return index_store::ptr(new metadata_index_store(tm_, bitmap_root_, entries_.size()));
		}

		virtual block_address get_root() const {
			return bitmap_root_;
		}

		virtual void check(block_counter &counter, block_address nr_index_entries) const {
			counter.inc(bitmap_root_);
			for (unsigned i = 0; i < entries_.size(); i++)
				if (entries_[i].blocknr_ != 0) // superblock
					counter.inc(entries_[i].blocknr_);
		}

	private:
		void load_ies() {
			block_manager<>::read_ref rr =
				tm_->read_lock(bitmap_root_, index_validator());

			metadata_index const *mdi = reinterpret_cast<metadata_index const *>(&rr.data());
			for (unsigned i = 0; i < entries_.size(); i++)
				index_entry_traits::unpack(*(mdi->index + i), entries_[i]);
		}

		transaction_manager::ptr tm_;
		block_address bitmap_root_;
		std::vector<index_entry> entries_;
	};

	class sm_metadata : public sm_disk_base {
	public:
		typedef boost::shared_ptr<sm_metadata> ptr;

		sm_metadata(transaction_manager::ptr tm)
			: sm_disk_base(index_store::ptr(new metadata_index_store(tm)), tm) {
		}

		sm_metadata(transaction_manager::ptr tm,
			    sm_root const &root)
			: sm_disk_base(mk_index_store(tm, root.bitmap_root_, root.nr_blocks_), tm, root) {
		}

	private:
		index_store::ptr mk_index_store(transaction_manager::ptr tm, block_address root, block_address nr_blocks) const {
			block_address nr_indexes = div_up<block_address>(nr_blocks, ENTRIES_PER_BLOCK);
			return index_store::ptr(new metadata_index_store(tm, root, nr_indexes));
		}
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
persistent_data::create_metadata_sm(transaction_manager::ptr tm, block_address nr_blocks)
{
	checked_space_map::ptr sm(new sm_metadata(tm));
	sm->extend(nr_blocks);
	return create_recursive_sm(sm);
}

checked_space_map::ptr
persistent_data::open_metadata_sm(transaction_manager::ptr tm, void *root)
{
	sm_root_disk d;
	sm_root v;

	::memcpy(&d, root, sizeof(d));
	sm_root_traits::unpack(d, v);

	return create_recursive_sm(checked_space_map::ptr(new sm_metadata(tm, v)));
}

//----------------------------------------------------------------

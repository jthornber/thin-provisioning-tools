#ifndef SPACE_MAP_DISK_H
#define SPACE_MAP_DISK_H

#include "space_map.h"
#include "transaction_manager.h"
#include "endian.h"
#include "space_map_disk_structures.h"
#include "math.h"

//----------------------------------------------------------------

namespace persistent_data {

	namespace sm_disk_detail {
		using namespace base;
		using namespace persistent_data;

		template <uint32_t BlockSize>
		class bitmap {
		public:
			bitmap(typename transaction_manager<BlockSize>::ptr tm,
			       index_entry const &ie)
				: tm_(tm),
				  ie_(ie) {
			}

			ref_t lookup(unsigned b) const {
				auto rr = tm_->read_lock(ie_.blocknr_);
				void const *bits = bitmap_data(rr);
				ref_t b1 = test_bit_le(bits, b * 2);
				ref_t b2 = test_bit_le(bits, b * 2 + 1);
				ref_t result = b2 ? 1 : 0;
				result |= b1 ? 0b10 : 0;
				return result;
			}

			void insert(unsigned b, ref_t n) {
				auto wr = tm_->shadow(ie_.blocknr_).first;
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
						return i;
					}
				}

				throw std::runtime_error("no free entry in bitmap");
			}

			index_entry const &get_ie() const {
				return ie_;
			}

		private:
			void *bitmap_data(typename transaction_manager<BlockSize>::write_ref &wr) {
				bitmap_header *h = reinterpret_cast<bitmap_header *>(&wr.data()[0]);
				return h + 1;
			}

			void const *bitmap_data(typename transaction_manager<BlockSize>::read_ref &rr) const {
				bitmap_header const *h = reinterpret_cast<bitmap_header const *>(&rr.data()[0]);
				return h + 1;
			}

			typename transaction_manager<BlockSize>::ptr tm_;
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

		template <uint32_t BlockSize>
		class sm_disk : public persistent_space_map {
		public:
			typedef boost::shared_ptr<sm_disk<BlockSize> > ptr;

			sm_disk(typename transaction_manager<BlockSize>::ptr tm,
				block_address nr_blocks)
				: tm_(tm),
				  entries_per_block_((BlockSize - sizeof(bitmap_header)) * 4),
				  nr_blocks_(0),
				  nr_allocated_(0),
				  bitmaps_(tm_, typename sm_disk_detail::index_entry_traits::ref_counter()),
				  ref_counts_(tm_, ref_count_traits::ref_counter()) {

				extend(nr_blocks);
			}

			sm_disk(typename transaction_manager<BlockSize>::ptr tm,
				sm_root const &root)
				: tm_(tm),
				  nr_blocks_(root.nr_blocks_),
				  nr_allocated_(root.nr_allocated_),
				  bitmaps_(tm_, root.bitmap_root_, typename sm_disk::index_entry_traits::ref_counter()),
				  ref_counts_(tm_, root.ref_count_root_, typename ref_count_traits::ref_counter()) {
			}

			block_address get_nr_blocks() const {
				return nr_blocks_;
			}

			block_address get_nr_free() const {
				return nr_blocks_ - nr_allocated_;
			}

			ref_t get_count(block_address b) const {
				auto count = lookup_bitmap(b);
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
					uint64_t key[1] = {index};
					auto mie = bitmaps_.lookup(key);

					if (!mie)
						throw runtime_error("bitmap entry missing from btree");

					bitmap<BlockSize> bm(tm_, *mie);
					block_address b = bm.find_free((index == nr_indexes - 1) ?
								       nr_blocks_ % entries_per_block_ : entries_per_block_);
					bitmaps_.insert(key, bm.get_ie());
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

			size_t root_size() {
				return sizeof(sm_root_disk);
			}

			void copy_root(void *dest, size_t len) {
				sm_root_disk d;
				sm_root v;

				if (len < sizeof(d))
					throw runtime_error("root too small");

				v.nr_blocks_ = nr_blocks_;
				v.nr_allocated_ = nr_allocated_;
				v.bitmap_root_ = bitmaps_.get_root();
				v.ref_count_root_ = ref_counts_.get_root();
				sm_root_traits::pack(v, d);
				::memcpy(dest, &d, sizeof(d));
			}

		private:
			void extend(block_address extra_blocks) {
				block_address nr_blocks = nr_blocks_ + extra_blocks;

				block_address bitmap_count = div_up<block_address>(nr_blocks, entries_per_block_);
				block_address old_bitmap_count = div_up<block_address>(nr_blocks_, entries_per_block_);
				for (block_address i = old_bitmap_count; i < bitmap_count; i++) {
					auto wr = tm_->new_block();

					struct index_entry ie;
					ie.blocknr_ = wr.get_location();
					ie.nr_free_ = i == (bitmap_count - 1) ?
						(nr_blocks % entries_per_block_) : entries_per_block_;
					ie.none_free_before_ = 0;

					uint64_t key[1] = {i};
					bitmaps_.insert(key, ie);
				}

				nr_blocks_ = nr_blocks;
			}

			ref_t lookup_bitmap(block_address b) const {
				uint64_t key[1] = {b / entries_per_block_};
				auto mindex = bitmaps_.lookup(key);
				if (!mindex)
					throw runtime_error("Couldn't lookup bitmap");

				bitmap<BlockSize> bm(tm_, *mindex);
				return bm.lookup(b % entries_per_block_);
			}

			void insert_bitmap(block_address b, unsigned n) {
				if (n > 3)
					throw runtime_error("bitmap can only hold 2 bit values");

				uint64_t key[1] = {b / entries_per_block_};
				auto mindex = bitmaps_.lookup(key);
				if (!mindex)
					throw runtime_error("Couldn't lookup bitmap");

				bitmap<BlockSize> bm(tm_, *mindex);
				bm.insert(b % entries_per_block_, n);
				bitmaps_.insert(key, bm.get_ie());
			}

			ref_t lookup_ref_count(block_address b) const {
				uint64_t key[1] = {b};
				auto mvalue = ref_counts_.lookup(key);
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

			typename transaction_manager<BlockSize>::ptr tm_;
			uint32_t entries_per_block_;
			block_address nr_blocks_;
			block_address nr_allocated_;

			btree<1, index_entry_traits, BlockSize> bitmaps_;
			btree<1, ref_count_traits, BlockSize> ref_counts_;
		};
	}

	template <uint32_t MetadataBlockSize>
	persistent_space_map::ptr
	create_disk_sm(typename transaction_manager<MetadataBlockSize>::ptr tm,
		       block_address nr_blocks)
	{
		using namespace sm_disk_detail;
		return typename persistent_space_map::ptr(
			new sm_disk<MetadataBlockSize>(tm, nr_blocks));
	}

	template <uint32_t MetadataBlockSize>
	persistent_space_map::ptr
	open_disk_sm(typename transaction_manager<MetadataBlockSize>::ptr tm,
		     void *root)
	{
		using namespace sm_disk_detail;

		sm_root_disk d;
		sm_root v;

		::memcpy(&d, root, sizeof(d));
		sm_root_traits::unpack(d, v);
		return typename persistent_space_map::ptr(
			new sm_disk<MetadataBlockSize>(tm, v));
	}
}

//----------------------------------------------------------------

#endif

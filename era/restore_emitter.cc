#include "era/restore_emitter.h"

#include "era/superblock.h"

using namespace era;
using namespace persistent_data;

//----------------------------------------------------------------

namespace {
	class restorer : public emitter {
	public:
		restorer(metadata &md)
			: md_(md),
			  in_superblock_(false),
			  in_writeset_(false),
			  in_era_array_(false) {
		}

		virtual void begin_superblock(std::string const &uuid,
					      uint32_t data_block_size,
					      pd::block_address nr_blocks,
					      uint32_t current_era) {
			superblock &sb = md_.sb_;
			memcpy(sb.uuid, reinterpret_cast<__u8 const *>(uuid.c_str()),
			       min<size_t>(sizeof(sb.uuid), uuid.length()));
			sb.data_block_size = data_block_size;
			sb.nr_blocks = nr_blocks;
			sb.current_era = current_era;

			nr_blocks = nr_blocks;
		}

		virtual void end_superblock() {
			if (!in_superblock_)
				throw runtime_error("missing superblock");

			md_.commit();
		}

		virtual void begin_writeset(uint32_t era, uint32_t nr_bits) {
			if (!in_superblock_)
				throw runtime_error("missing superblock");

			in_writeset_ = true;
			era_ = era;

			bits_.reset(new bitset(md_.tm_));
			bits_->grow(nr_bits, false);
		}

		virtual void writeset_bit(uint32_t bit, bool value) {
			bits_->set(bit, value);
		}

		virtual void end_writeset() {
			in_writeset_ = false;

			era_detail e;
			e.nr_bits = bits_->get_nr_bits();
			e.writeset_root = bits_->get_root();

			uint64_t key[1] = {era_};
			md_.writeset_tree_->insert(key, e);
		}

		virtual void begin_era_array() {
			if (!in_superblock_)
				throw runtime_error("missing superblock");

			in_era_array_ = true;
		}

		virtual void era(pd::block_address block, uint32_t era) {
			if (!in_era_array_)
				throw runtime_error("missing era array");

			md_.era_array_->set(block, era);
		}

		virtual void end_era_array() {
			in_era_array_ = false;
		}

	private:
		metadata &md_;

		bool in_superblock_;

		bool in_writeset_;
		uint32_t era_;
		pd::bitset::ptr bits_;

		bool in_era_array_;
		uint32_t nr_blocks_;
	};
}

//----------------------------------------------------------------

emitter::ptr
era::create_restore_emitter(metadata &md)
{
	return emitter::ptr(new restorer(md));
}

//----------------------------------------------------------------

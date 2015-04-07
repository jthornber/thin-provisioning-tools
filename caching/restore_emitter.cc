#include "caching/restore_emitter.h"
#include "caching/superblock.h"
#include "caching/mapping_array.h"

using namespace caching;
using namespace std;
using namespace superblock_damage;

//----------------------------------------------------------------

namespace {
	class restorer : public emitter {
	public:
		restorer(metadata::ptr md, bool clean_shutdown)
			: in_superblock_(false),
			  md_(md),
			  clean_shutdown_(clean_shutdown) {
		}

		virtual void begin_superblock(std::string const &uuid,
					      pd::block_address block_size,
					      pd::block_address nr_cache_blocks,
					      std::string const &policy,
					      size_t hint_width) {

			superblock &sb = md_->sb_;
			strncpy((char *) sb.policy_name, policy.c_str(), sizeof(sb.policy_name));
			memset(sb.policy_version, 0, sizeof(sb.policy_version)); // FIXME: should come from xml
			sb.policy_hint_size = hint_width;
			md_->setup_hint_array(hint_width);

			sb.data_block_size = block_size;
			sb.cache_blocks = nr_cache_blocks;

			struct mapping unmapped_value;
			unmapped_value.oblock_ = 0;
			unmapped_value.flags_ = 0;
			md_->mappings_->grow(nr_cache_blocks, unmapped_value);

			vector<unsigned char> hint_value(hint_width, '\0');
			md_->hints_->grow(nr_cache_blocks, hint_value);
		}

		virtual void end_superblock() {
			md_->commit(clean_shutdown_);
		}

		virtual void begin_mappings() {
			// noop
		}

		virtual void end_mappings() {
			// noop
		}

		virtual void mapping(pd::block_address cblock,
				     pd::block_address oblock,
				     bool dirty) {
			caching::mapping m;
			m.oblock_ = oblock;
			m.flags_ = M_VALID;

			if (dirty)
				m.flags_ = m.flags_ | M_DIRTY;

			md_->mappings_->set(cblock, m);
		}

		virtual void begin_hints() {
			// noop
		}

		virtual void end_hints() {
			// noop
		}

		virtual void hint(pd::block_address cblock,
				  vector<unsigned char> const &data) {
			md_->hints_->set_hint(cblock, data);
		}

		virtual void begin_discards() {
			// noop
		}

		virtual void end_discards() {
			// noop
		}

		virtual void discard(block_address dblock, block_address dblock_e) {
			while (dblock != dblock_e) {
				md_->discard_bits_->set(dblock, true);
				dblock++;
			}
		}

	private:
		bool in_superblock_;
		metadata::ptr md_;
		bool clean_shutdown_;
	};
}

//----------------------------------------------------------------

emitter::ptr
caching::create_restore_emitter(metadata::ptr md, bool clean_shutdown)
{
	return emitter::ptr(new restorer(md, clean_shutdown));
}

//----------------------------------------------------------------

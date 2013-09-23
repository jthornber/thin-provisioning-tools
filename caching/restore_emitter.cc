#include "caching/restore_emitter.h"
#include "caching/superblock.h"
#include "caching/mapping_array.h"

using namespace caching;
using namespace mapping_array_detail;
using namespace std;
using namespace superblock_detail;

//----------------------------------------------------------------

namespace {
	class restorer : public emitter {
	public:
		restorer(metadata::ptr md)
			: in_superblock_(false),
			  md_(md) {
		}

		virtual ~restorer() {
		}

		virtual void begin_superblock(std::string const &uuid,
					      pd::block_address block_size,
					      pd::block_address nr_cache_blocks,
					      std::string const &policy) {

			superblock &sb = md_->sb_;

			sb.flags = 0;
			memset(sb.uuid, 0, sizeof(sb.uuid));
			sb.magic = caching::superblock_detail::SUPERBLOCK_MAGIC;
			sb.version = 0; // FIXME: fix
			// strncpy(sb.policy_name, policy.c_str(), sizeof(sb.policy_name));
			memset(sb.policy_version, 0, sizeof(sb.policy_version));
			sb.policy_hint_size = 0; // FIXME: fix

			memset(sb.metadata_space_map_root, 0, sizeof(sb.metadata_space_map_root));
			sb.mapping_root = 0;
			sb.hint_root = 0;

			sb.discard_root = 0;
			sb.discard_block_size = 0;
			sb.discard_nr_blocks = 0;

			sb.data_block_size = block_size;
			sb.metadata_block_size = 0;
			sb.cache_blocks = nr_cache_blocks;

			sb.compat_flags = 0;
			sb.compat_ro_flags = 0;
			sb.incompat_flags = 0;

			sb.read_hits = 0;
			sb.read_misses = 0;
			sb.write_hits = 0;
			sb.write_misses = 0;

			struct mapping unmapped_value;
			unmapped_value.oblock_ = 0;
			unmapped_value.flags_ = 0;
			md_->mappings_->grow(nr_cache_blocks, unmapped_value);
		}

		virtual void end_superblock() {
			md_->commit();
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
			mapping_array_detail::mapping m;
			m.oblock_ = oblock;
			m.flags_ = M_VALID;

			if (dirty)
				m.flags_ = m.flags_ | M_DIRTY;

			md_->mappings_->set(cblock, m);
		}

		virtual void begin_hints() {
		}

		virtual void end_hints() {
		}

		virtual void hint(pd::block_address cblock,
				  std::string const &data) {
		}

	private:
		bool in_superblock_;
		metadata::ptr md_;
	};
}

//----------------------------------------------------------------

emitter::ptr
caching::create_restore_emitter(metadata::ptr md)
{
	return emitter::ptr(new restorer(md));
}

//----------------------------------------------------------------

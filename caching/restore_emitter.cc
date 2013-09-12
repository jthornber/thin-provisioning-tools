#include "caching/restore_emitter.h"
#include "caching/superblock.h"

using namespace std;
using namespace caching;

//----------------------------------------------------------------

namespace {
	using namespace superblock_detail;

	class restorer : public emitter {
	public:
		restorer(metadata::ptr md)
			: md_(md) {
		}

		virtual void begin_superblock(std::string const &uuid,
					      pd::block_address block_size,
					      pd::block_address nr_cache_blocks,
					      std::string const &policy) {
		}

		virtual void end_superblock() {
		}

		virtual void begin_mappings() {
		}

		virtual void end_mappings() {
		}

		virtual void mapping(pd::block_address cblock,
				     pd::block_address oblock,
				     bool dirty) {
		}

		virtual void begin_hints() {
		}

		virtual void end_hints() {
		}

		virtual void hint(pd::block_address cblock,
				  std::string const &data) {
		}

	private:
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

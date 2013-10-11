#ifndef CACHE_EMITTER_H
#define CACHE_EMITTER_H

#include "persistent-data/block.h"

#include <boost/shared_ptr.hpp>
#include <vector>

//----------------------------------------------------------------

namespace caching {
	namespace pd = persistent_data;

	class emitter {
	public:
		typedef boost::shared_ptr<emitter> ptr;

		virtual ~emitter() {}

		virtual void begin_superblock(std::string const &uuid,
					      pd::block_address block_size,
					      pd::block_address nr_cache_blocks,
					      std::string const &policy,
					      size_t hint_width) = 0;

		virtual void end_superblock() = 0;

		virtual void begin_mappings() = 0;
		virtual void end_mappings() = 0;

		virtual void mapping(pd::block_address cblock,
				     pd::block_address oblock,
				     bool dirty) = 0;

		virtual void begin_hints() = 0;
		virtual void end_hints() = 0;

		virtual void hint(pd::block_address cblock,
				  std::vector<unsigned char> const &data) = 0;

		virtual void begin_discards() = 0;
		virtual void end_discards() = 0;

		virtual void discard(pd::block_address dblock_begin,
				     pd::block_address dblock_end) = 0;
	};
}

//----------------------------------------------------------------

#endif

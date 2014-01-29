#ifndef ERA_EMITTER_H
#define ERA_EMITTER_H

#include "persistent-data/block.h"

//----------------------------------------------------------------

namespace era {
	namespace pd = persistent_data;

	class emitter {
	public:
		typedef boost::shared_ptr<emitter> ptr;

		virtual ~emitter() {}

		virtual void begin_superblock(std::string const &uuid,
					      uint32_t data_block_size,
					      pd::block_address nr_blocks,
					      uint32_t current_era) = 0;
		virtual void end_superblock() = 0;

		virtual void begin_writeset(uint32_t era, uint32_t nr_bits) = 0;
		virtual void writeset_bit(uint32_t bit, bool value) = 0;
		virtual void end_writeset() = 0;

		virtual void begin_era_array() = 0;
		virtual void era(pd::block_address block, uint32_t era) = 0;
		virtual void end_era_array() = 0;
	};
}

//----------------------------------------------------------------

#endif

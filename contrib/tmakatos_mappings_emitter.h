#ifndef _TMAKATOS_MAPPINGS_EMITTER_H_
#define _TMAKATOS_MAPPINGS_EMITTER_H_

#include "thin-provisioning/emitter.h"
#include <climits>

using namespace std;
using namespace thin_provisioning;

namespace tmakatos_mappings_emitter {
	class binary_emitter : public emitter {
	public:
		binary_emitter(ostream &out);

		void begin_superblock(string const &uuid,
				      uint64_t time,
				      uint64_t trans_id,
				      boost::optional<uint32_t> flags,
				      boost::optional<uint32_t> version,
				      uint32_t data_block_size,
				      uint64_t nr_data_blocks,
				      boost::optional<uint64_t> metadata_snap);

		void end_superblock();

		void begin_device(uint32_t dev_id,
				  uint64_t mapped_blocks,
				  uint64_t trans_id,
				  uint64_t creation_time,
				  uint64_t snap_time);

		void end_device();

		void begin_named_mapping(string const &name);

		void end_named_mapping();

		void identifier(string const &name);

		void range_map(uint64_t origin_begin, uint64_t data_begin,
				uint32_t, uint64_t len);

		void single_map(uint64_t origin_block, uint64_t data_block,
				uint32_t);

	private:
		ostream &out_;
		uint64_t prev;
		void _range_map(uint64_t data_begin, uint64_t len);
	};
}

#endif /* _TMAKATOS_MAPPINGS_EMITTER_H_ */

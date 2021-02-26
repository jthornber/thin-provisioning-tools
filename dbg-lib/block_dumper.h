#ifndef DBG_BLOCK_DUMPER_H
#define DBG_BLOCK_DUMPER_H

#include "persistent-data/block.h"

//----------------------------------------------------------------

namespace dbg {
	class block_dumper {
	public:
		typedef std::shared_ptr<block_dumper> ptr;

		// pass the read_ref by reference since the caller already held the ref-count
		virtual void show(persistent_data::block_manager::read_ref &rr,
				  std::ostream &out) = 0;
	};
}

//----------------------------------------------------------------

#endif

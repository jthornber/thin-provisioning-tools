#ifndef BLOCK_CACHE_COPIER_H
#define BLOCK_CACHE_COPIER_H

#include "block_cache.h"

#include <string>

//----------------------------------------------------------------

namespace bcache {
	class copier {
	public:
		// block size in sectors
		copier(std::string const &src, std::string const &dest,
		       unsigned block_size);
		~copier();

		// Returns the number of sectors copied
		unsigned copy(block_address from, block_address to);

		unsigned get_block_size() const;

	private:
		unsigned block_size_;
	};
}

//----------------------------------------------------------------

#endif

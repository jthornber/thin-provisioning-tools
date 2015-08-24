#ifndef THIN_PROVISIONING_CACHE_STREAM_H
#define THIN_PROVISIONING_CACHE_STREAM_H

#include "thin-provisioning/chunk_stream.h"

//----------------------------------------------------------------

namespace thin_provisioning {
	using namespace bcache;

	class cache_stream : public chunk_stream {
	public:
		cache_stream(std::string const &path,
			     block_address block_size,
			     size_t cache_mem);

		virtual block_address nr_chunks() const;
		virtual void rewind();
		virtual bool advance(block_address count = 1ull);
		virtual block_address index() const;
		virtual chunk const &get() const;

	private:
		void load(block_address b);

		block_address block_size_;
		block_address nr_blocks_;
		block_address cache_blocks_;
		int fd_;
		validator::ptr v_;
		std::auto_ptr<block_cache> cache_;

		block_address current_index_;
		block_cache::auto_block current_block_;
		chunk current_chunk_;
	};
}

//----------------------------------------------------------------

#endif

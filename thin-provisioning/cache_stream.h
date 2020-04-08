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

		block_address size() const;

		virtual void rewind();

		virtual bool next(block_address count = 1ull);
		virtual bool eof() const;

		virtual chunk const &get();
		virtual void put(chunk const &c);

	private:
		struct chunk_wrapper {
			chunk_wrapper(cache_stream &parent);

			block_cache::auto_block block_;
			chunk c_;
		};

		friend struct chunk_wrapper;

		block_address block_size_;
		block_address nr_blocks_;
		block_address cache_blocks_;

		file_utils::file_descriptor fd_;
		validator::ptr v_;
		std::unique_ptr<block_cache> cache_;

		block_address current_index_;
	};
}

//----------------------------------------------------------------

#endif

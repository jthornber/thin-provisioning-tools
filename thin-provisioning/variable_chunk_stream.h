#ifndef THIN_PROVISIONING_VARIABLE_CHUNK_STREAM_H
#define THIN_PROVISIONING_VARIABLE_CHUNK_STREAM_H

#include "base/rolling_hash.h"
#include "thin-provisioning/chunk_stream.h"

//----------------------------------------------------------------

namespace thin_provisioning {
	class variable_chunk_stream : public chunk_stream {
	public:
		// window_size must be a power of 2
		variable_chunk_stream(chunk_stream &stream, unsigned window_size);
		~variable_chunk_stream();

		// FIXME: we don't know in advance how many chunks we will have
		virtual void rewind();
		virtual bool next(bcache::block_address count = 1ull);
		virtual bool eof() const;
		virtual bcache::block_address index() const;
		virtual chunk const &get();
		virtual void put(chunk const &c);

	private:
		bool next_big_chunk();
		bool advance_one();
		void put_big_chunk();

		bcache::block_address index_;
		base::content_based_hash h_;

		chunk_stream &stream_;
		chunk const *big_chunk_;

		uint8_t *little_b_, *little_e_;
		chunk little_chunk_;
	};
}

//----------------------------------------------------------------

#endif

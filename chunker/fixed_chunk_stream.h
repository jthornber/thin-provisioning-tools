#ifndef CHUNKER_FIXED_CHUNK_STREAM_H
#define CHUNKER_FIXED_CHUNK_STREAM_H

#include "chunker/chunk_stream.h"

//----------------------------------------------------------------

namespace thin_provisioning {
	class fixed_chunk_stream : public chunk_stream {
	public:
		fixed_chunk_stream(chunk_stream &stream, unsigned chunk_size);
		~fixed_chunk_stream();

		virtual bcache::block_address size() const;
		virtual void rewind();
		virtual bool next(bcache::block_address count = 1ull);
		virtual bool eof() const;
		virtual chunk const &get();
		virtual void put(chunk const &c);

	private:
		bool next_big_chunk();
		bool advance_one();
		void put_big_chunk();

		bcache::block_address index_;

		chunk_stream &stream_;
		unsigned chunk_size_;
		chunk const *big_chunk_;

		uint8_t *little_b_, *little_e_, *last_hashed_;
		chunk little_chunk_;
	};
}

//----------------------------------------------------------------

#endif

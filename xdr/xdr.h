#ifndef XDR_XDR_H
#define XDR_XDR_H

#include <boost/shared_ptr.hpp>
#include <stdint.h>
#include <string>
#include <vector>

//----------------------------------------------------------------

namespace xdr {
	// Opaque values are a special case since we will not manipulate
	// the data, and so want to avoid spurious copying.  No reference
	// counting is done, the caller is reposible for ensuring the
	// original data buffer lives as long as the opaque object.

	class xdr_opaque {
	public:
		xdr_opaque(uint8_t *begin, uint8_t *end);

		// FIXME: add comparison operators since we'll use this to
		// store the SHA1 hashes

		uint32_t size() const;
		void resize(uint32_t s) const;

		uint8_t const *begin() const;
		uint8_t const *end() const;
		uint8_t *begin();
		uint8_t *end();

	private:
		uint8_t *begin_, *end_;
	};

	//--------------------------------

	class xdr_write_buffer {
	public:
		typedef boost::shared_ptr<xdr_write_buffer> ptr;

		xdr_write_buffer(uint8_t *begin, uint8_t *end);

		uint8_t const *begin() const;
		uint8_t const *end() const;

		void encode(bool n);
		void encode(int32_t n);
		void encode(uint32_t n);

		void encode(int64_t n);
		void encode(uint64_t n);

		void encode_fixed(uint32_t expected_len, xdr_opaque const &data);
		void encode_variable(uint32_t max, xdr_opaque const &data);

		void encode(uint32_t max, std::string const &str);

	private:
		// Pads to be a multiple of 4
		void push_bytes(uint8_t const *begin, uint8_t const *end);
		void push_word(uint32_t w);
		void push_hyper(uint64_t w);

		uint8_t *begin_, *end_, *current_;
		uint32_t padding_;
	};

	class xdr_read_buffer {
	public:
		typedef boost::shared_ptr<xdr_read_buffer> ptr;

		xdr_read_buffer(uint8_t const *begin, uint8_t const *end);

		// end - begin must be a multiple of 4
		void pop(uint8_t *begin, uint8_t *end);

		void decode(bool &n);
		void decode(int32_t &n);
		void decode(uint32_t &n);

		void decode(int64_t &n);
		void decode(uint64_t &n);

		void decode_fixed(uint32_t len, xdr_opaque &data);
		void decode_variable(uint32_t max, xdr_opaque &data);

		void decode(uint32_t max, std::string &str);


	private:
		void pop_bytes(uint8_t *begin, uint8_t *end);
		uint32_t pop_word();
		uint64_t pop_hyper();
		void inc_current(size_t len);

		uint8_t const *begin_, *end_, *current_;
	};
}

//----------------------------------------------------------------

#endif

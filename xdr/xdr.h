#ifndef XDR_XDR_H
#define XDR_XDR_H

#include <boost/shared_ptr.hpp>
#include <boost/variant.hpp>
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
		xdr_opaque()
		: begin_(0),
		  end_(0) {
		}

		xdr_opaque(uint8_t const *begin, uint8_t const *end)
		: begin_(begin),
		  end_(end) {
		  }

		// FIXME: add comparison operators since we'll use this to
		// store the SHA1 hashes

		uint32_t size() const {
			return end_ - begin_;
		}

		uint8_t const *begin() const {
			return begin_;
		}

		uint8_t const *end() const {
			return end_;
		}

	private:
		uint8_t const *begin_, *end_;
	};

	//--------------------------------

	class xdr_write_buffer {
	public:
		xdr_write_buffer(uint8_t *begin, uint8_t *end);

		uint8_t const *begin() const;
		uint8_t const *end() const;

		void push_bytes(uint8_t const *begin, uint8_t const *end);
		void push_word(uint32_t w);
		void push_hyper(uint64_t w);

	private:
		uint8_t *begin_, *end_, *current_;
		uint32_t padding_;
	};

	void encode_bool(xdr_write_buffer &buf, bool n);
	void encode_int32_t(xdr_write_buffer &buf, int32_t n);
	void encode_uint32_t(xdr_write_buffer &buf, uint32_t n);

	void encode_int64_t(xdr_write_buffer &buf, int64_t n);
	void encode_uint64_t(xdr_write_buffer &buf, uint64_t n);

	void encode_fixed_opaque(xdr_write_buffer &buf, uint32_t expected_len, xdr_opaque const &data);
	void encode_variable_opaque(xdr_write_buffer &buf, uint32_t max, xdr_opaque const &data);

	void encode_string(xdr_write_buffer &buf, std::string const &str, uint32_t max = 0);

	//--------------------------------

	class xdr_read_buffer {
	public:
		xdr_read_buffer(uint8_t const *begin, uint8_t const *end);

		// end - begin must be a multiple of 4
		void pop(uint8_t *begin, uint8_t *end);

		void pop_bytes(uint8_t *begin, uint8_t *end);
		void pop_bytes_no_copy(uint32_t len, uint8_t const **begin, uint8_t const **end);
		uint32_t pop_word();
		uint64_t pop_hyper();

	private:
		void ensure_data(size_t len) const;
		void inc_current(size_t len);

		uint8_t const *begin_, *end_, *current_;
	};

	void decode_bool(xdr_read_buffer &buf, bool &n);
	void decode_int32_t(xdr_read_buffer &buf, int32_t &n);
	void decode_uint32_t(xdr_read_buffer &buf, uint32_t &n);

	void decode_int64_t(xdr_read_buffer &buf, int64_t &n);
	void decode_uint64_t(xdr_read_buffer &buf, uint64_t &n);

	void decode_fixed_opaque(xdr_read_buffer &buf, uint32_t len, xdr_opaque &data);
	void decode_variable_opaque(xdr_read_buffer &buf, xdr_opaque &data, uint32_t max = 0);

	void decode_string(xdr_read_buffer &buf, std::string &str, uint32_t max = 0);
}

//----------------------------------------------------------------

#endif

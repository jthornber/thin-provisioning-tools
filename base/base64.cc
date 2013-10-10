#include "base/base64.h"

#include <boost/optional.hpp>
#include <sstream>
#include <stdexcept>

using namespace base;
using namespace boost;
using namespace std;

//----------------------------------------------------------------

namespace {
	char const *table_ = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

	struct index_set {
		unsigned nr_valid_;
		unsigned index_[4];
	};

	index_set split1(unsigned char c) {
		index_set r;

		r.nr_valid_ = 2;
		r.index_[0] = c >> 2;
		r.index_[1] = (c & 3) << 4;

		return r;
	}

	index_set split2(unsigned char c1, unsigned char c2) {
		index_set r;

		r.nr_valid_ = 3;
		r.index_[0] = c1 >> 2;
		r.index_[1] = ((c1 & 3) << 4) | (c2 >> 4);
		r.index_[2] = (c2 & 15) << 2;

		return r;
	}

	index_set split3(unsigned char c1, unsigned char c2, unsigned c3) {
		index_set r;

		r.nr_valid_ = 4;
		r.index_[0] = c1 >> 2;
		r.index_[1] = ((c1 & 3) << 4) | (c2 >> 4);
		r.index_[2] = ((c2 & 15) << 2) | (c3 >> 6);
		r.index_[3] = c3 & 63;

		return r;
	}

	index_set split(vector<unsigned char> const &raw, unsigned index) {
		unsigned remaining = std::min<unsigned>(raw.size() - index, 3);

		switch (remaining) {
		case 1:
			return split1(raw.at(index));

		case 2:
			return split2(raw.at(index), raw.at(index + 1));

		case 3:
			return split3(raw.at(index), raw.at(index + 1), raw.at(index + 2));
		}

		throw std::runtime_error("internal error, in split");
	}

	optional<unsigned> char_to_index(char c) {
		// FIXME: very slow
		for (unsigned i = 0; i < 64; i++)
			if (table_[i] == c)
				return optional<unsigned>(i);

		return optional<unsigned>();
	}

	decoded_or_error success(vector<unsigned char> const &decoded) {
		return decoded_or_error(decoded);
	}

	decoded_or_error fail(string msg) {
		return decoded_or_error(msg);
	}

	decoded_or_error fail_char(char c) {
		ostringstream msg;
		msg << "bad input character: '" << c << "'";
		return fail(msg.str());
	}

	decoded_or_error decode_quad(char c1, char c2, char c3, char c4) {
		typedef optional<unsigned> oi;
		unsigned char d1, d2, d3;
		vector<unsigned char> decoded;

		oi i1 = char_to_index(c1);
		if (!i1)
			return fail_char(c1);

		oi i2 = char_to_index(c2);
		if (!i2)
			return fail_char(c2);

		d1 = (*i1 << 2) | (*i2 >> 4);
		decoded.push_back(d1);

		d2 = (*i2 & 15) << 4;

		if (c3 == '=') {
			// FIXME: I really think the push should be here
//			decoded.push_back(d2);
			return success(decoded);
		}

		oi i3 = char_to_index(c3);
		if (!i3)
			return fail_char(c3);

		d2 = d2 | (*i3 >> 2);
		decoded.push_back(d2);

		d3 = (*i3 & 3) << 6;

		if (c4 == '=') {
			// FIXME: I really think the push should be here
//			decoded.push_back(d3);
			return success(decoded);
		}

		oi i4 = char_to_index(c4);
		if (!i4)
			return fail_char(c4);

		d3 = d3 | *i4;
		decoded.push_back(d3);

		return success(decoded);
	}
}

//----------------------------------------------------------------

string
base::base64_encode(vector<unsigned char> const &raw)
{
	string r;

	for (unsigned i = 0; i < raw.size(); i += 3) {
		unsigned j;
		index_set is = split(raw, i);

		for (j = 0; j < is.nr_valid_; j++)
			r.push_back(table_[is.index_[j]]);

		for (; j < 4; j++)
			r.push_back('=');
	}

	return r;
}

base::decoded_or_error
base::base64_decode(string const &encoded)
{
	if (encoded.length() % 4)
		return decoded_or_error("bad input length");

	vector<unsigned char> decoded;

	for (unsigned i = 0; i < encoded.length(); i += 4) {
		decoded_or_error doe = decode_quad(encoded[i], encoded[i + 1], encoded[i + 2], encoded[i + 3]);

		vector<unsigned char> *v = get<vector<unsigned char> >(&doe);
		if (!v)
			return doe;

		decoded.insert(decoded.end(), v->begin(), v->end());
	}

	return decoded_or_error(decoded);
}

//----------------------------------------------------------------

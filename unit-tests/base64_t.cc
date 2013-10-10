#include "gmock/gmock.h"
#include "base/base64.h"

#include <stdexcept>
#include <stdlib.h>

using namespace base;
using namespace boost;
using namespace std;
using namespace testing;

//----------------------------------------------------------------

namespace {
	typedef vector<unsigned char> bytes;

	char const *wikipedia_examples[] = {
		"any carnal pleasure.", "YW55IGNhcm5hbCBwbGVhc3VyZS4=",
		"any carnal pleasure", "YW55IGNhcm5hbCBwbGVhc3VyZQ==",
		"any carnal pleasur", "YW55IGNhcm5hbCBwbGVhc3Vy",
		"any carnal pleasu", "YW55IGNhcm5hbCBwbGVhc3U=",
		"any carnal pleas", "YW55IGNhcm5hbCBwbGVhcw==",
		"pleasure.", "cGxlYXN1cmUu",
		"leasure.", "bGVhc3VyZS4=",
		"easure.", "ZWFzdXJlLg==",
		"asure.", "YXN1cmUu",
		"sure.", "c3VyZS4="
	};

	void assert_fails(decoded_or_error const &eoe, string const &msg) {
		ASSERT_THAT(get<string>(eoe), Eq(msg));
	}
};

//----------------------------------------------------------------

TEST(Base64Tests, encoding_an_empty_string)
{
	bytes bs;
	ASSERT_THAT(base64_encode(bs), Eq(string()));
}

TEST(Base64Tests, decoding_an_empty_string)
{
	bytes bs;
	ASSERT_THAT(get<vector<unsigned char> >(base64_decode("")), Eq(bs));
}

TEST(Base64Tests, encode_single_byte)
{
	bytes bs(1);
	bs[0] = 0;

	ASSERT_THAT(base64_encode(bs), Eq(string("AA==")));
}

TEST(Base64Tests, encode_double_byte)
{
	bytes bs(2, 0);
	ASSERT_THAT(base64_encode(bs), Eq(string("AAA=")));
}

TEST(Base64Tests, encode_triple_byte)
{
	bytes bs(3, 0);
	ASSERT_THAT(base64_encode(bs), Eq(string("AAAA")));
}

TEST(Base64Tests, longer_encodings)
{
	for (unsigned example = 0; example < 5; example++) {
		char const *in = wikipedia_examples[example * 2];
		char const *out = wikipedia_examples[example * 2 + 1];
		unsigned len = strlen(in);
		bytes bs(len);
		for (unsigned b = 0; b < len; b++)
			bs.at(b) = in[b];

		ASSERT_THAT(base64_encode(bs), Eq(string(out)));
	}
}

TEST(Base64Tests, decoding_fails_with_bad_size_input)
{
	char const *err = "bad input length";

	assert_fails(base64_decode("AAA"), err);
	assert_fails(base64_decode("AA"), err);
	assert_fails(base64_decode("A"), err);
}

TEST(Base64Tests, encode_decode_cycle)
{
	for (unsigned example = 0; example < 5; example++) {
		char const *in = wikipedia_examples[example * 2];
		unsigned len = strlen(in);
		bytes bs(len);
		for (unsigned b = 0; b < len; b++)
			bs.at(b) = in[b];

		decoded_or_error doe = base64_decode(base64_encode(bs));
		ASSERT_THAT(get<vector<unsigned char> >(doe), Eq(bs));
	}
}

TEST(Base64Tests, random_data)
{
	for (unsigned len = 1; len < 17; len++) {
		for (unsigned example = 0; example < 10000; example++) {
			vector<unsigned char> raw(len);

			for (unsigned i = 0; i < len; i++)
				raw.at(i) = ::rand() % 256;

			decoded_or_error doe = base64_decode(base64_encode(raw));
			ASSERT_THAT(get<vector<unsigned char> >(doe), Eq(raw));
		}
	}
}

//----------------------------------------------------------------

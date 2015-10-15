#include "gmock/gmock.h"

#include "unit-tests/xdr1.h"
#include <memory>
#include <stdexcept>

using namespace xdr;
using namespace std;
using namespace testing;
using namespace unit_tests_xdr1;

//----------------------------------------------------------------

namespace {
        class XDR1Tests : public Test {
        public:
                XDR1Tests()
                        : space_(1024 * 1024, 0),
                          write_buf_(space_.data(), space_.data() + space_.size())
                {
                }

                void complete_write() {
                        read_buf_.reset(new xdr_read_buffer(write_buf_.begin(), write_buf_.end()));
                }

                xdr_read_buffer &read_buf() {
                        if (!read_buf_.get())
                                throw runtime_error("complete_write() not called");
                        return *read_buf_;
                }

                vector<uint8_t> space_;
                xdr_write_buffer write_buf_;
                auto_ptr<xdr_read_buffer> read_buf_;
        };
}

//----------------------------------------------------------------

TEST_F(XDR1Tests, empty_test)
{
}

TEST_F(XDR1Tests, encode_unsigned)
{
        uint32_t v(1234);
        encode_uint32_t(write_buf_, v);
        EXPECT_THAT(write_buf_.end() - write_buf_.begin(), Eq(4));
        complete_write();
        uint32_t v2 = 0;
        decode_uint32_t(read_buf(), v2);
        EXPECT_THAT(v2, Eq(v));
}

TEST_F(XDR1Tests, encode_int)
{
        int32_t v(-1234);
        encode_int32_t(write_buf_, v);
        EXPECT_THAT(write_buf_.end() - write_buf_.begin(), Eq(4));
        complete_write();
        int32_t v2 = 0;
        decode_int32_t(read_buf(), v2);
        EXPECT_THAT(v2, Eq(v));
}

TEST_F(XDR1Tests, encode_hyper)
{
        uint64_t v(12345678900ULL);
        encode_uint64_t(write_buf_, v);
        EXPECT_THAT(write_buf_.end() - write_buf_.begin(), Eq(8));
        complete_write();
        uint64_t v2 = 0;
        decode_uint64_t(read_buf(), v2);
        EXPECT_THAT(v2, Eq(v));
}

TEST_F(XDR1Tests, encode_string)
{
        string text("The quick brown fox.");
        encode_string(write_buf_, text);
        complete_write();
        string text2;
        decode_string(read_buf(), text2);
        EXPECT_THAT(text2, Eq(text));
}

TEST_F(XDR1Tests, encode_overlong_string)
{
	string str("the quick brown fox jumps");
	ASSERT_THROW(encode_small_string_t(write_buf_, str), runtime_error);
}

TEST_F(XDR1Tests, encode_foo)
{
        foo v(1234);
        encode_foo(write_buf_, v);
        complete_write();
        foo v2;
        decode_foo(read_buf(), v2);
        EXPECT_THAT(v2, Eq(v));
}

TEST_F(XDR1Tests, encode_fixed_array)
{
        fixed_array_t v = {1, 2, 3, 4, 5};
        encode_fixed_array_t(write_buf_, v);
        complete_write();
        fixed_array_t v2;
        decode_fixed_array_t(read_buf(), v2);
        for (unsigned i = 0; i < 5; i++)
                EXPECT_THAT(v2[i], Eq(v[i]));
}

TEST_F(XDR1Tests, encode_var_array)
{
	unsigned len = 123;
        var_array_t v(len, 0);

	for (unsigned i = 0; i < v.size(); i++)
		v[i] = i * 2;
        encode_var_array_t(write_buf_, v);
        complete_write();
        var_array_t v2;
        decode_var_array_t(read_buf(), v2);
	EXPECT_THAT(v2, Eq(v));
}

TEST_F(XDR1Tests, encode_overlarge_var_array)
{
	unsigned len = 5000;
	var_array_t v(len, 0);
	ASSERT_THROW(encode_var_array_t(write_buf_, v), runtime_error);
}

TEST_F(XDR1Tests, decode_overlarge_var_array)
{
	unsigned len = 5000;
	big_var_array_t v(len, 0);
	encode_big_var_array_t(write_buf_, v);
	complete_write();
	var_array_t v2;
	ASSERT_THROW(decode_var_array_t(read_buf(), v2), runtime_error);
}

//----------------------------------------------------------------

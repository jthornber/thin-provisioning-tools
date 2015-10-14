#include "gmock/gmock.h"

#include "unit-tests/xdr1.h"

using namespace xdr;
using namespace std;
using namespace testing;

//----------------------------------------------------------------

namespace {
        class XDR1Tests : public Test {
        public:
                XDR1Tests()
                        : space_(1024, 0),
                          write_buf_(space_.data(), space_.data() + space_.size()),
                          read_buf_(space_.data(), space_.data() + space_.size()) {
                }

                vector<uint8_t> space_;
                xdr_write_buffer write_buf_;
                xdr_read_buffer read_buf_;
        };
}

//----------------------------------------------------------------

TEST_F(XDR1Tests, empty_test)
{
}

TEST_F(XDR1Tests, encode_unsigned)
{
        using namespace unit_tests_xdr1;

        {
                uint32_t v(1234);
                encode(write_buf_, v);
                EXPECT_THAT(write_buf_.end() - write_buf_.begin(), Eq(4));
        }

        {
                uint32_t v = 0;
                decode(read_buf_, v);
                EXPECT_THAT(v, Eq(1234));
        }
}

TEST_F(XDR1Tests, encode_hyper)
{
        using namespace unit_tests_xdr1;

        {
                uint64_t v(12345678900ULL);
                encode(write_buf_, v);
                EXPECT_THAT(write_buf_.end() - write_buf_.begin(), Eq(8));
        }

        {
                uint64_t v = 0;
                decode(read_buf_, v);
                EXPECT_THAT(v, Eq(12345678900ULL));
        }
}

//----------------------------------------------------------------

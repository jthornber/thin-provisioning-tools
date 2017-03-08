#include "gmock/gmock.h"

#include "base/rolling_hash.h"

using namespace base;
using namespace boost;
using namespace std;
using namespace testing;

//----------------------------------------------------------------

namespace {
	class RollingHashTests : public Test {
	public:
		RollingHashTests()
			: window_size_(4096),
			  rhash_(window_size_) {
		}

		typedef vector<uint8_t> bytes;
		bytes random_bytes(unsigned count) {
			bytes v(count, 0);

			for (unsigned i = 0; i < count; i++)
				v[i] = random_byte();

			return v;
		}

		uint8_t random_byte() const {
			return random() % 256;
		}

		void apply_bytes(bytes const &bs) {
			for (unsigned i = 0; i < bs.size(); i++)
				rhash_.step(bs[i]);
		}

		unsigned window_size_;
		rolling_hash rhash_;
	};

	class ContentBasedHashTests : public Test {
	public:
		ContentBasedHashTests()
			: window_size_(8192),
			  h_(window_size_) {
		}

		typedef vector<uint8_t> bytes;
		bytes random_bytes(unsigned count) {
			bytes v(count, 0);

			for (unsigned i = 0; i < count; i++)
				v[i] = random_byte();

			return v;
		}

		uint8_t random_byte() const {
			return random() % 256;
		}

		unsigned window_size_;
		content_based_hash h_;
	};
}

//----------------------------------------------------------------

TEST_F(RollingHashTests, ctr)
{
}

//--------------------------------

TEST_F(RollingHashTests, hash_changes)
{
	bytes bs = random_bytes(window_size_ * 100);

	uint32_t prev = rhash_.get_hash();
	for (unsigned i = 0; i < bs.size(); i++) {
		rhash_.step(bs[i]);
		ASSERT_NE(rhash_.get_hash(), prev);
		prev = rhash_.get_hash();
	}
}

TEST_F(RollingHashTests, hash_repeats)
{
	bytes bs = random_bytes(window_size_);

	apply_bytes(bs);
	uint32_t h1 = rhash_.get_hash();
	apply_bytes(bs);

	ASSERT_EQ(rhash_.get_hash(), h1);
}

TEST_F(RollingHashTests, reset_is_deterministic)
{
	uint8_t bytes[] = "lksdfuwerh,sdg";

	for (unsigned i = 0; i < sizeof(bytes) - 1; i++)
		rhash_.step(bytes[i]);

	uint32_t h1 = rhash_.get_hash();

	rhash_.reset();

	for (unsigned i = 0; i < sizeof(bytes) - 1; i++)
		rhash_.step(bytes[i]);

	uint32_t h2 = rhash_.get_hash();

	ASSERT_EQ(h1, h2);
}

//----------------------------------------------------------------

TEST_F(ContentBasedHashTests, ctr)
{
}

TEST_F(ContentBasedHashTests, chunk_limits_respected)
{
	unsigned min = 100000, max = 0;

	bytes bs = random_bytes(1024 * 1024 * 100);
	vector<unsigned> counts(window_size_, 0);

	for (unsigned i = 0; i < bs.size(); i++) {
		optional<unsigned> b = h_.step(bs[i]);
		if (b) {
			counts[*b]++;

			if (*b < min)
				min = *b;

			if (*b > max)
				max = *b;
		}
	}

#if 1
	for (unsigned i = 0; i < counts.size(); i++)
		cerr << i << ": " << counts[i] << "\n";

	cerr << "min: " << min << ", max: " << max << "\n";
#endif
}

//----------------------------------------------------------------

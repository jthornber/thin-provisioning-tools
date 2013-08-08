#include "gmock/gmock.h"

#include "persistent-data/space-maps/subtracting_span_iterator.h"
#include "persistent-data/run_set.h"

using namespace std;
using namespace persistent_data;
using namespace testing;

//----------------------------------------------------------------

namespace {
	typedef space_map::span span;
	typedef space_map::maybe_span maybe_span;

	class regular_span_iterator : public space_map::span_iterator {
	public:
		regular_span_iterator(block_address span_length,
				      block_address gap_length,
				      unsigned span_count)
			: span_length_(span_length),
			  gap_length_(gap_length),
			  span_count_(span_count),
			  current_span_(0) {
		}

		virtual maybe_span first() {
			current_span_ = 0;
			return current_span();
		}


		virtual maybe_span next() {
			current_span_++;
			return current_span();
		}

	private:
		maybe_span current_span() const {
			if (current_span_ >= span_count_)
				return maybe_span();

			block_address begin = (span_length_ + gap_length_) * current_span_;
			block_address end = begin + span_length_;
			return maybe_span(space_map::span(begin, end));
		}

		block_address span_length_;
		block_address gap_length_;
		unsigned span_count_;

		unsigned current_span_;
	};

	class span_iterator_mock : public space_map::span_iterator {
	public:
		span_iterator_mock() {
			Sequence dummy;

			EXPECT_CALL(*this, first()).
				InSequence(dummy).
				WillOnce(Return(maybe_span(span(23, 47))));
			EXPECT_CALL(*this, next()).
				InSequence(dummy).
				WillOnce(Return(maybe_span(span(59, 103))));
			EXPECT_CALL(*this, next()).
				InSequence(dummy).
				WillOnce(Return(maybe_span()));
		}

		MOCK_METHOD0(first, maybe_span());
		MOCK_METHOD0(next, maybe_span());
	};

	class SpanItTests : public Test {
	public:
		subtracting_span_iterator run() {
			span_iterator_mock mock_it;
			return subtracting_span_iterator(1000, mock_it, forbidden);
		}

		base::run_set<block_address> forbidden;
	};

	ostream &operator <<(ostream &out, maybe_span const &m) {
		out << "maybe_span[";
		if (m)
			out << m->first << ", " << m->second;
		out << "]";

		return out;
	}
}

//----------------------------------------------------------------

TEST(RegularSpanItTests, regular_span_iterator)
{
	regular_span_iterator it(5, 3, 5);

	static struct {
		block_address b;
		block_address e;
	} expected[] = {
		{0, 5},
		{8, 13},
		{16, 21},
		{24, 29},
		{32, 37}
	};

	ASSERT_THAT(it.first(), Eq(maybe_span(span(expected[0].b, expected[0].e))));

	for (unsigned count = 1; count < 5; count++)
		ASSERT_THAT(it.next(), Eq(maybe_span(span(expected[count].b, expected[count].e))));

	ASSERT_THAT(it.next(), Eq(maybe_span()));
}

TEST_F(SpanItTests, sub_it_with_no_removed_blocks)
{
	subtracting_span_iterator it = run();
	ASSERT_THAT(it.first(), Eq(maybe_span(span(23, 47))));
	ASSERT_THAT(it.next(), Eq(maybe_span(span(59, 103))));
}

TEST_F(SpanItTests, sub_it_with_removed_first_block)
{
	forbidden.add(23);

	subtracting_span_iterator it = run();
	ASSERT_THAT(it.first(), Eq(maybe_span(span(24, 47))));
	ASSERT_THAT(it.next(), Eq(maybe_span(span(59, 103))));
}

TEST_F(SpanItTests, sub_it_with_removed_run_overlapping_front_of_first_block)
{
	for (block_address i = 19; i < 26; i++)
		forbidden.add(i);

	subtracting_span_iterator it = run();
	ASSERT_THAT(it.first(), Eq(maybe_span(span(26, 47))));
	ASSERT_THAT(it.next(), Eq(maybe_span(span(59, 103))));
}

TEST_F(SpanItTests, sub_it_with_removed_mid_block)
{
	forbidden.add(40);

	subtracting_span_iterator it = run();
	ASSERT_THAT(it.first(), Eq(maybe_span(span(23, 40))));
	ASSERT_THAT(it.next(), Eq(maybe_span(span(41, 47))));
	ASSERT_THAT(it.next(), Eq(maybe_span(span(59, 103))));
}

TEST_F(SpanItTests, sub_it_with_removed_run_mid_block)
{
	for (block_address i = 26; i < 36; i++)
		forbidden.add(i);

	subtracting_span_iterator it = run();
	ASSERT_THAT(it.first(), Eq(maybe_span(span(23, 26))));
	ASSERT_THAT(it.next(), Eq(maybe_span(span(36, 47))));
	ASSERT_THAT(it.next(), Eq(maybe_span(span(59, 103))));
}

TEST_F(SpanItTests, sub_it_with_removed_end_block)
{
	forbidden.add(46);

	subtracting_span_iterator it = run();
	ASSERT_THAT(it.first(), Eq(maybe_span(span(23, 46))));
	ASSERT_THAT(it.next(), Eq(maybe_span(span(59, 103))));
}

TEST_F(SpanItTests, sub_it_with_removed_run_end_block)
{
	for (block_address i = 26; i < 50; i++)
		forbidden.add(i);

	subtracting_span_iterator it = run();
	ASSERT_THAT(it.first(), Eq(maybe_span(span(23, 26))));
	ASSERT_THAT(it.next(), Eq(maybe_span(span(59, 103))));
}

TEST_F(SpanItTests, sub_it_with_removed_run_overlapping_2_blocks)
{
	for (block_address i = 26; i < 70; i++)
		forbidden.add(i);

	subtracting_span_iterator it = run();
	ASSERT_THAT(it.first(), Eq(maybe_span(span(23, 26))));
	ASSERT_THAT(it.next(), Eq(maybe_span(span(70, 103))));
}

TEST_F(SpanItTests, sub_it_with_removed_intermediate_blocks)
{
	forbidden.add(53);
	forbidden.add(54);
	forbidden.add(57);
	forbidden.add(58);

	subtracting_span_iterator it = run();
	ASSERT_THAT(it.first(), Eq(maybe_span(span(23, 47))));
	ASSERT_THAT(it.next(), Eq(maybe_span(span(59, 103))));
}

//----------------------------------------------------------------

#include "gmock/gmock.h"

#include "base/sequence_generator.h"

using namespace base;
using namespace std;
using namespace testing;

//----------------------------------------------------------------

namespace {
	class SequenceGeneratorTests : public Test {
	};

	class ForwardSequenceGeneratorTests : public Test {
	public:
		ForwardSequenceGeneratorTests() {
		}

		void reset(uint64_t begin, uint64_t size, uint64_t step) {
			gen_ = create_forward_sequence_generator(
					begin, size, step);

			// the maximum value is expected to be less than
			// (begin + round_down(size, step))
			begin_ = begin;
			end_ = begin + (size / step) * step;
			step_ = step;
		}

		void test_full_sequence() {
			for (uint64_t i = begin_; i < end_; i += step_)
				ASSERT_EQ(i, gen_->next());
		}

	private:
		sequence_generator::ptr gen_;
		uint64_t begin_;
		uint64_t end_;
		uint64_t step_;
	};

	class RandomSequenceGeneratorTests : public Test {
		struct span_tracker {
			span_tracker(uint64_t begin, uint64_t step)
				: last_(begin),
				  step_(step),
				  len_(1),
				  max_len_(1) {
			}

			void append(uint64_t n) {
				if (n == last_ + step_)
					++len_;
				else
					len_ = 1;

				if (len_ > max_len_)
					max_len_ = len_;

				last_ = n;
			}

			uint64_t last_;
			uint64_t step_;
			unsigned len_;
			unsigned max_len_;
		};

	public:
		RandomSequenceGeneratorTests() {
		}

		void reset(uint64_t begin, uint64_t size, uint64_t step,
			   unsigned seq_nr = 1) {
			gen_ = create_random_sequence_generator(
					begin, size, step, seq_nr);

			// the maximum value is expected to be less than
			// (begin + round_down(size, step))
			begin_ = begin;
			nr_steps_ = size / step;
			step_ = step;
			seq_nr_ = seq_nr;
		}

		void test_full_sequence() {
			std::set<uint64_t> values;

			// generate the whole sequence
			uint64_t n = gen_->next();
			values.insert(n);
			span_tracker tracker(n, step_);

			for (uint64_t i = 1; i < nr_steps_; i++) {
				n = gen_->next();
				values.insert(n);
				tracker.append(n);
			}

			// verify the generated sequence
			ASSERT_EQ(values.size(), nr_steps_); // assert no duplicates

			set<uint64_t>::const_iterator it = values.begin();
			uint64_t end = begin_ + nr_steps_ * step_;
			for (uint64_t v = begin_; v < end; v += step_) {
				ASSERT_EQ(v, *it);
				++it;
			}

			// FIXME: refine this assertion
			// The maximum span length depends on the randomness.
			// It should be greater than half of the desired length,
			// if the probabilities are evenly distributed.
			if (seq_nr_ > 1) {
				ASSERT_TRUE(tracker.max_len_ >= seq_nr_ / 2);
			}
		}

	private:
		sequence_generator::ptr gen_;
		uint64_t begin_;
		uint64_t nr_steps_;
		uint64_t step_;
		unsigned seq_nr_;
	};
}

//----------------------------------------------------------------

TEST_F(SequenceGeneratorTests, create_with_valid_params)
{
	// step < size
	create_forward_sequence_generator(0, 10, 2);

	// boundary test: step == size
	create_forward_sequence_generator(0, 10, 10);

	// step < size
	create_random_sequence_generator(0, 10, 2);

	// boundary test: step == size
	create_random_sequence_generator(0, 10, 10);

	// seq_nr < nr_steps
	create_random_sequence_generator(0, 10, 2, 2);

	// boundary test: seq_nr == nr_steps
	create_random_sequence_generator(0, 10, 2, 5);
}

TEST_F(SequenceGeneratorTests, create_with_invalid_params)
{
	// zero size or step
	ASSERT_THROW(create_forward_sequence_generator(0, 1, 0), std::runtime_error);
	ASSERT_THROW(create_forward_sequence_generator(0, 0, 1), std::runtime_error);

	// step > size
	ASSERT_THROW(create_forward_sequence_generator(0, 10, 11), std::runtime_error);

	// zero size, step, or seq_nr
	ASSERT_THROW(create_random_sequence_generator(0, 1, 0), std::runtime_error);
	ASSERT_THROW(create_random_sequence_generator(0, 0, 1), std::runtime_error);
	ASSERT_THROW(create_random_sequence_generator(0, 1, 1, 0), std::runtime_error);

	// step > size
	ASSERT_THROW(create_random_sequence_generator(0, 10, 11), std::runtime_error);

	// seq_nr > nr_steps
	ASSERT_THROW(create_random_sequence_generator(0, 10, 2, 6), std::runtime_error);
}

TEST_F(ForwardSequenceGeneratorTests, forward_sequence_aligned)
{
	reset(50, 100, 2); // begin & size are aligned to step
	test_full_sequence();
	test_full_sequence(); // wrap-around
}

TEST_F(ForwardSequenceGeneratorTests, forward_sequence_unaligned)
{
	reset(50, 100, 7); // begin & size are not aligned to step
	test_full_sequence();
	test_full_sequence(); // wrap-around
}

TEST_F(RandomSequenceGeneratorTests, random_sequence_aligned)
{
	reset(50, 100, 2); // begin & size are aligned to step
	test_full_sequence();
	test_full_sequence(); // wrap-around
}

TEST_F(RandomSequenceGeneratorTests, random_sequence_unaligned)
{
	reset(50, 100, 7); // begin & size are not aligned to step
	test_full_sequence();
	test_full_sequence(); // wrap-around
}

TEST_F(RandomSequenceGeneratorTests, random_sequence_with_span)
{
	reset(50, 10000, 2, 10); // begin & size are aligned to step
	test_full_sequence();
	test_full_sequence(); // wrap-around
}

//----------------------------------------------------------------

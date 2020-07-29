#include "base/run_set.h"
#include "base/sequence_generator.h"
#include <stdexcept>

//----------------------------------------------------------------

namespace {
	// - The maximum generated value is not greater than (begin+size)
	// - The generated values are not aligned to the step, if the begin
	//   value is not aligned.
	class forward_sequence_generator: public base::sequence_generator {
	public:
		forward_sequence_generator(uint64_t begin,
					   uint64_t size,
					   uint64_t step)
			: begin_(begin),
			  step_(step),
			  current_(begin),
			  rounds_(0)
		{
			verify_parameters(size, step);
			end_ = begin + (size / step) * step;
		}

		forward_sequence_generator()
			: begin_(0), end_(1), step_(1),
			  current_(0), rounds_(0) {
		}

		uint64_t next() {
			uint64_t r = current_;
			current_ += step_;
			if (current_ >= end_) {
				current_ = begin_;
				++rounds_;
			}
			return r;
		}

		void reset(uint64_t begin, uint64_t size, uint64_t step = 1) {
			verify_parameters(size, step);

			begin_ = begin;
			end_ = begin + (size / step) * step;
			step_ = step;
			current_ = begin;
			rounds_ = 0;
		}

		static void verify_parameters(uint64_t size, uint64_t step) {
			if (!size || !step)
				throw std::runtime_error("size or step must be non-zero");

			if (size < step)
				throw std::runtime_error("size must be greater than the step");
		}

		uint64_t get_rounds() {
			return rounds_;
		}

	private:
		uint64_t begin_;
		uint64_t end_;
		uint64_t step_;
		uint64_t current_;
		uint64_t rounds_;
	};

	// - The maximum generated value is not greater than (begin+size)
	// - The generated values are not aligned to the step, if the begin
	//   value is not aligned.
	class random_sequence_generator: public base::sequence_generator {
	public:
		// TODO: load random seeds
		random_sequence_generator(uint64_t begin,
					  uint64_t size,
					  uint64_t step,
					  unsigned seq_nr = 1)
			: begin_(begin),
			  step_(step),
			  max_forward_steps_(seq_nr),
			  nr_generated_(0)
		{
			if (!size || !step || !seq_nr)
				throw std::runtime_error("size, step, or forward steps must be non-zero");

			if (size < step)
				throw std::runtime_error("size must be greater than the step");

			nr_steps_ = size / step;

			if (!max_forward_steps_ || max_forward_steps_ > nr_steps_)
				throw std::runtime_error("invalid number of forward steps");

			if (max_forward_steps_ > 1)
				reset_forward_generator();
		}

		uint64_t next() {
			// FIXME: eliminate if-else
			uint64_t step_idx = (max_forward_steps_ > 1) ?
				next_forward_step() : next_random_step();

			return begin_ + step_idx * step_;
		}

	private:
		void reset_forward_generator() {
			uint64_t begin = peek_random_step();

			unsigned seq_nr = (std::rand() % max_forward_steps_) + 1;
			base::run_set<uint64_t>::const_iterator it = rand_map_.upper_bound(begin);
			if (it != rand_map_.end())
				seq_nr = std::min<uint64_t>(seq_nr, *it->begin_ - begin);
			else
				seq_nr = std::min<uint64_t>(seq_nr, nr_steps_ - begin);

			forward_gen_.reset(begin, seq_nr);
		}

		uint64_t next_forward_step() {
			uint64_t step_idx = forward_gen_.next();
			consume_random_map(step_idx);

			if (forward_gen_.get_rounds())
				reset_forward_generator();

			return step_idx;
		}

		uint64_t next_random_step() {
			uint64_t step_idx = peek_random_step();
			consume_random_map(step_idx);

			return step_idx;
		}

		void consume_random_map(uint64_t step_idx) {
			rand_map_.add(step_idx);

			++nr_generated_;

			// wrap-around
			if (nr_generated_ == nr_steps_) {
				rand_map_.clear();
				nr_generated_ = 0;
			}
		}

		uint64_t peek_random_step() const {
			uint64_t step_idx;

			bool found = true;
			while (found) {
				step_idx = std::rand() % nr_steps_;
				found = rand_map_.member(step_idx);
			}

			return step_idx;
		}

		uint64_t begin_;
		uint64_t nr_steps_;
		uint64_t step_;
		unsigned max_forward_steps_;

		base::run_set<uint64_t> rand_map_;
		uint64_t nr_generated_;
		forward_sequence_generator forward_gen_;
	};
}

//----------------------------------------------------------------

base::sequence_generator::ptr
base::create_forward_sequence_generator(uint64_t begin,
					uint64_t size,
					uint64_t step)
{
	return sequence_generator::ptr(
			new forward_sequence_generator(begin, size, step));
}

base::sequence_generator::ptr
base::create_random_sequence_generator(uint64_t begin,
				       uint64_t size,
				       uint64_t step,
				       unsigned seq_nr)
{
	return sequence_generator::ptr(
			new random_sequence_generator(begin, size, step, seq_nr));
}

//----------------------------------------------------------------

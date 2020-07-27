#include "base/io_generator.h"
#include "persistent-data/run_set.h"
#include <stdexcept>
#include <cstdlib>
#include <cstring>

using namespace base;

//----------------------------------------------------------------

namespace {
	std::pair<char const*, io_pattern::pattern> patterns[] = {
		{"read", io_pattern::READ},
		{"write", io_pattern::WRITE},
		{"trim", io_pattern::TRIM},
		{"readwrite", io_pattern::READ_WRITE},
		{"trimwrite", io_pattern::TRIM_WRITE},
		{"randread", io_pattern::RAND_READ},
		{"randwrite", io_pattern::RAND_WRITE},
		{"randtrim", io_pattern::RAND_TRIM},
		{"randrw", io_pattern::RAND_RW},
		{"randtw", io_pattern::RAND_TW}
	};

	unsigned const nr_patterns = sizeof(patterns) / sizeof(patterns[0]);

	//--------------------------------

	class sequence_generator {
	public:
		typedef std::shared_ptr<sequence_generator> ptr;

		virtual uint64_t next() = 0;
	};

	// - The maximum generated value is not greater than (begin+size)
	// - The generated values are not aligned to the step, if the begin
	//   value is not aligned.
	class forward_sequence_generator: public sequence_generator {
	public:
		forward_sequence_generator(uint64_t begin,
					   uint64_t size,
					   uint64_t step)
			: begin_(begin),
			  end_(begin + (size / step) * step),
			  step_(step),
			  current_(begin),
			  rounds_(0) {
			if (size < step)
				throw std::runtime_error("size must be greater than the step");
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
			begin_ = begin;
			end_ = begin + (size / step) * step;
			step_ = step;
			current_ = begin;
			rounds_ = 0;
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
	class random_sequence_generator: public sequence_generator {
	public:
		// TODO: load random seeds
		random_sequence_generator(uint64_t begin,
					  uint64_t size,
					  uint64_t step,
					  unsigned seq_nr = 1)
			: begin_(begin),
			  nr_steps_(size / step),
			  step_(step),
			  max_forward_steps_(seq_nr),
			  nr_generated_(0)
		{
			if (!max_forward_steps_ || max_forward_steps_ > nr_steps_)
				throw std::runtime_error("invalid number of forward steps");

			if (max_forward_steps_ > 1)
				reset_forward_generator();
		}

		uint64_t next() {
			// FIXME: eliminate if-else
			uint64_t step_idx = (max_forward_steps_ > 1) ?
				next_forward_step() : next_random_step();
			rand_map_.add(step_idx);
			++nr_generated_;

			// wrap-around
			if (nr_generated_ == nr_steps_) {
				rand_map_.clear();
				nr_generated_ = 0;
			}

			return begin_ + step_idx * step_;
		}

	private:
		void reset_forward_generator() {
			uint64_t begin = next_random_step();
			unsigned seq_nr = (std::rand() % max_forward_steps_) + 1;
			forward_gen_.reset(begin, seq_nr);
		}

		uint64_t next_forward_step() {
			uint64_t step_idx;

			bool found = true;
			while (found) {
				step_idx = forward_gen_.next();
				found = rand_map_.member(step_idx);

				if (found || forward_gen_.get_rounds())
					reset_forward_generator();
			}

			return step_idx;
		}

		uint64_t next_random_step() const {
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

	//--------------------------------

	class op_generator {
	public:
		typedef std::shared_ptr<op_generator> ptr;

		op_generator(base::req_op op1)
			: op1_(op1), op2_(op1), op1_pct_(100) {
		}

		op_generator(base::req_op op1,
			     base::req_op op2,
			     unsigned op1_pct)
			: op1_(op1), op2_(op2), op1_pct_(op1_pct) {
			if (op1_pct > 100)
				throw std::runtime_error("invalid percentage");
		}

		base::req_op next_op() {
			if (static_cast<unsigned>(std::rand()) % 100 > op1_pct_)
				return op2_;
			return op1_;
		}

	private:
		base::req_op op1_;
		base::req_op op2_;
		unsigned op1_pct_;
	};

	//--------------------------------

	class base_io_generator: public io_generator {
	public:
		base_io_generator(io_generator_options const &opts);
		virtual bool next(base::io &next_io);

	private:
		sequence_generator::ptr
		create_offset_generator(io_generator_options const &opts);

		op_generator::ptr
		create_op_generator(io_generator_options const &opts);

		sequence_generator::ptr offset_gen_;
		op_generator::ptr op_gen_;
		sector_t block_size_;
		size_t io_size_finished_;
		size_t io_size_total_;
	};

	base_io_generator::base_io_generator(io_generator_options const &opts)
		: offset_gen_(create_offset_generator(opts)),
		  op_gen_(create_op_generator(opts)),
		  block_size_(opts.block_size_),
		  io_size_finished_(0),
		  io_size_total_(opts.io_size_) {
	}

	bool base_io_generator::next(base::io &next_io) {
		if (io_size_finished_ >= io_size_total_)
			return false;

		next_io.op_ = op_gen_->next_op();
		next_io.sector_ = offset_gen_->next();
		next_io.size_ = block_size_;

		io_size_finished_ += block_size_;

		return true;
	}

	sequence_generator::ptr
	base_io_generator::create_offset_generator(io_generator_options const &opts) {
		sequence_generator *gen;

		if (opts.pattern_.is_random())
			gen = new random_sequence_generator(opts.offset_,
					opts.size_, opts.block_size_,
					opts.nr_seq_blocks_);
		else
			gen = new forward_sequence_generator(opts.offset_,
					opts.size_, opts.block_size_);

		return sequence_generator::ptr(gen);
	}

	op_generator::ptr
	base_io_generator::create_op_generator(io_generator_options const &opts) {
		// FIXME: elimiate the switch-case and hide enum values
		switch (opts.pattern_.val_) {
		case io_pattern::READ:
		case io_pattern::RAND_READ:
			return op_generator::ptr(new op_generator(base::REQ_OP_READ));
		case io_pattern::WRITE:
		case io_pattern::RAND_WRITE:
			return op_generator::ptr(new op_generator(base::REQ_OP_WRITE));
		case io_pattern::TRIM:
		case io_pattern::RAND_TRIM:
			return op_generator::ptr(new op_generator(base::REQ_OP_DISCARD));
		case io_pattern::READ_WRITE:
		case io_pattern::RAND_RW:
			return op_generator::ptr(new op_generator(base::REQ_OP_READ,
								  base::REQ_OP_WRITE,
								  50));
		case io_pattern::TRIM_WRITE:
		case io_pattern::RAND_TW:
			return op_generator::ptr(new op_generator(base::REQ_OP_DISCARD,
								  base::REQ_OP_WRITE,
								  50));
		default:
			throw std::runtime_error("unknown pattern");
		}
	}
}

//----------------------------------------------------------------

io_pattern::io_pattern()
	: val_(pattern::READ) {
}

io_pattern::io_pattern(char const *pattern) {
	parse(pattern);
}

void
io_pattern::parse(char const *pattern) {
	bool found = false;
	unsigned i = 0;
	for (i = 0; i < nr_patterns; i++) {
		if (!strcmp(patterns[i].first, pattern)) {
			found = true;
			break;
		}
	}

	if (!found)
		throw std::runtime_error("unknow pattern");

	val_ = patterns[i].second;
}

bool
io_pattern::is_random() const {
	return val_ & pattern::RANDOM;
}

//----------------------------------------------------------------

io_generator::ptr
base::create_io_generator(io_generator_options const &opts) {
	return io_generator::ptr(new base_io_generator(opts));
}

//----------------------------------------------------------------

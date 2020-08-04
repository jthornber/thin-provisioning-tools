#include "base/io_generator.h"
#include "base/sequence_generator.h"
#include <chrono>
#include <random>
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

	class op_generator {
	public:
		typedef std::shared_ptr<op_generator> ptr;

		op_generator(base::req_op op1)
			: op1_(op1), op2_(op1), op1_pct_(100),
			  rand_seed_(std::chrono::high_resolution_clock::now().time_since_epoch().count()),
			  op_engine_(rand_seed_) {
		}

		op_generator(base::req_op op1,
			     base::req_op op2,
			     unsigned op1_pct)
			: op1_(op1), op2_(op2), op1_pct_(op1_pct),
			  rand_seed_(std::chrono::high_resolution_clock::now().time_since_epoch().count()),
			  op_engine_(rand_seed_) {
			if (op1_pct > 100)
				throw std::runtime_error("invalid percentage");
		}

		base::req_op next_op() {
			if (op_engine_() % 100 > op1_pct_)
				return op2_;
			return op1_;
		}

	private:
		base::req_op op1_;
		base::req_op op2_;
		unsigned op1_pct_;
		uint64_t rand_seed_;

		std::mt19937 op_engine_;
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
		sequence_generator::ptr gen;

		if (opts.pattern_.is_random())
			gen = create_random_sequence_generator(opts.offset_,
					opts.size_, opts.block_size_,
					opts.nr_seq_blocks_);
		else
			gen = create_forward_sequence_generator(opts.offset_,
					opts.size_, opts.block_size_);

		return gen;
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

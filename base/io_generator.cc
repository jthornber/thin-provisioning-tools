#include "base/io_generator.h"
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

	class offset_generator {
	public:
		typedef std::shared_ptr<offset_generator> ptr;

		virtual base::sector_t next_offset() = 0;
	};

	class sequential_offset_generator: public offset_generator {
	public:
		sequential_offset_generator(base::sector_t offset,
					    base::sector_t size,
					    base::sector_t block_size)
			: block_size_(block_size),
			  begin_(offset),
			  end_(offset + size),
			  current_(offset) {
			if (size < block_size)
				throw std::runtime_error("size must be greater than block_size");
		}

		base::sector_t next_offset() {
			sector_t r = current_;
			current_ += block_size_;
			if (current_ > end_)
				current_ = begin_;
			return r;
		}

	private:
		unsigned block_size_;
		base::sector_t begin_;
		base::sector_t end_;
		base::sector_t current_;
	};

	class random_offset_generator: public offset_generator {
	public:
		random_offset_generator(sector_t offset,
					sector_t size,
					sector_t block_size)
			: block_begin_(offset / block_size),
			  nr_blocks_(size / block_size),
			  block_size_(block_size) {
		}

		sector_t next_offset() {
			return ((std::rand() % nr_blocks_) + block_begin_) * block_size_;
		}

	private:
		uint64_t block_begin_;
		uint64_t nr_blocks_;
		unsigned block_size_;
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
		offset_generator::ptr
		create_offset_generator(io_generator_options const &opts);

		op_generator::ptr
		create_op_generator(io_generator_options const &opts);

		offset_generator::ptr offset_gen_;
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
		next_io.sector_ = offset_gen_->next_offset();
		next_io.size_ = block_size_;

		io_size_finished_ += block_size_;

		return true;
	}

	offset_generator::ptr
	base_io_generator::create_offset_generator(io_generator_options const &opts) {
		if (opts.pattern_.is_random())
			return offset_generator::ptr(
				new random_offset_generator(opts.offset_,
							    opts.size_,
							    opts.block_size_));

		return offset_generator::ptr(
			new sequential_offset_generator(opts.offset_,
							opts.size_,
							opts.block_size_));
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

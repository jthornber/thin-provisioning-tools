#ifndef BASE_IO_GENERATOR_H
#define BASE_IO_GENERATOR_H

#include "base/io.h"
#include <memory>

//----------------------------------------------------------------

namespace base {
	struct io_pattern {
		enum pattern {
			READ = 1 << 1,
			WRITE = 1 << 2,
			TRIM = 1 << 3,
			RANDOM = 1 << 8,
			READ_WRITE = READ | WRITE,
			TRIM_WRITE = WRITE | TRIM,
			RAND_READ = READ | RANDOM,
			RAND_WRITE = WRITE | RANDOM,
			RAND_TRIM = TRIM | RANDOM,
			RAND_RW = READ_WRITE | RANDOM,
			RAND_TW = TRIM_WRITE | RANDOM,
		};

		io_pattern();
		io_pattern(char const *pattern);
		void parse(char const *pattern);
		bool is_random() const;

		pattern val_;
	};

	struct io_generator_options {
		io_pattern pattern_;
		sector_t offset_;
		sector_t block_size_;
		sector_t size_;
		sector_t io_size_;
	};

	class io_generator {
	public:
		typedef std::shared_ptr<io_generator> ptr;

		virtual bool next(base::io &next_io) = 0;
	};

	io_generator::ptr
	create_io_generator(io_generator_options const &opts);
}

//----------------------------------------------------------------

#endif

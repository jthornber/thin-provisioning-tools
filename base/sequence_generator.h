#ifndef BASE_SEQUENCE_GENERATOR_H
#define BASE_SEQUENCE_GENERATOR_H

#include <memory>

//----------------------------------------------------------------

namespace base {
	class sequence_generator {
	public:
		typedef std::shared_ptr<sequence_generator> ptr;

		virtual uint64_t next() = 0;
	};

	sequence_generator::ptr
	create_forward_sequence_generator(uint64_t begin, uint64_t size,
			uint64_t step);

	sequence_generator::ptr
	create_random_sequence_generator(uint64_t begin, uint64_t size,
			uint64_t step, unsigned seq_nr = 1);
}

//----------------------------------------------------------------

#endif

#ifndef BASE_ALIGNED_MEMORY_H
#define BASE_ALIGNED_MEMORY_H

#include <boost/noncopyable.hpp>

//----------------------------------------------------------------

namespace base {
	class aligned_memory : boost::noncopyable {
	public:
		aligned_memory(size_t len, size_t alignment);
		~aligned_memory();

		void *data() {
			return data_;
		}

	private:
		void *data_;
	};
}

//----------------------------------------------------------------

#endif

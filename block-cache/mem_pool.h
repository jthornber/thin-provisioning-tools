#ifndef BLOCK_CACHE_MEM_POOL_H
#define BLOCK_CACHE_MEM_POOL_H

#include <boost/intrusive/list.hpp>
#include <boost/optional.hpp>
#include <list>

namespace bi = boost::intrusive;

//----------------------------------------------------------------

namespace bcache {
	// FIXME: move to base?

	namespace mempool_detail {
		struct alloc_block : public bi::list_base_hook<bi::link_mode<bi::normal_link>> {
		};
	};

	class mempool {
	public:
		// alignment must be a power of 2
		mempool(size_t block_size, size_t total_mem, size_t alignment = 8);
		~mempool();

		void *alloc();
		void free(void *data);

	private:
		static void *alloc_aligned(size_t len, size_t alignment);

		using block_list = bi::list<mempool_detail::alloc_block>;

		void *mem_;
		block_list free_;

		//----------------

		mempool(mempool const &) = delete;
		mempool &operator =(mempool const &) = delete;
	};
}

//----------------------------------------------------------------

#endif

#include "block-cache/mem_pool.h"

#include <sstream>
#include <stdexcept>
#include <stdlib.h>

using namespace bcache;
using namespace boost;
using namespace mempool_detail;
using namespace std;

//----------------------------------------------------------------

mempool::mempool(size_t block_size, size_t total_mem, size_t alignment)
{
	mem_ = alloc_aligned(total_mem, alignment);

	unsigned nr_blocks = total_mem / block_size;
	for (auto i = 0u; i < nr_blocks; i++)
		free(static_cast<unsigned char *>(mem_) + (block_size * i));
}

mempool::~mempool()
{
	free_.clear();
	::free(mem_);
}

void *
mempool::alloc()
{
	if (free_.empty())
		return nullptr;

	mempool_detail::alloc_block &b = free_.front();
	free_.pop_front();
	return reinterpret_cast<void *>(&b);
}

void
mempool::free(void *data)
{
	mempool_detail::alloc_block *b = reinterpret_cast<mempool_detail::alloc_block *>(data);
	free_.push_front(*b);
}

void *
mempool::alloc_aligned(size_t len, size_t alignment)
{
	void *result = NULL;
	int r = posix_memalign(&result, alignment, len);
	if (r) {
		ostringstream out;
		out << "posix_memalign failed: len = " << len << ", alignment = " << alignment << ", r = " << r << "\n";
		throw runtime_error(out.str());
	}

	return result;
}

//----------------------------------------------------------------


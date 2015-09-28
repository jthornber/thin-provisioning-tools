#include "chunker/cache_stream.h"
#include "persistent-data/file_utils.h"

using namespace chunker;
using namespace std;
using namespace persistent_data;

//----------------------------------------------------------------

namespace {
	int open_file(string const &path) {
		int fd = ::open(path.c_str(), O_RDONLY | O_DIRECT | O_EXCL, 0666);
		if (fd < 0)
			syscall_failed("open",
				       "Note: you cannot run this tool with these options on live metadata.");

		return fd;
	}
}

//----------------------------------------------------------------

cache_stream::cache_stream(string const &path,
			   block_address block_size,
			   size_t cache_mem)
	: block_size_(block_size),
	  nr_blocks_(get_nr_blocks(path, block_size)),

	  // hack because cache uses LRU rather than MRU
	  cache_blocks_((cache_mem / block_size) / 2u),
	  fd_(open_file(path)),
	  v_(new bcache::noop_validator()),
	  cache_(new block_cache(fd_, block_size / 512, nr_blocks_, cache_mem)),
	  current_index_(0) {

	rewind();
}

block_address
cache_stream::size() const
{
	return nr_blocks_ * block_size_;
}

void
cache_stream::rewind()
{
	current_index_ = 0;

	for (block_address i = 1; i < min(cache_blocks_, nr_blocks_); i++)
		cache_->prefetch(i);
}

bool
cache_stream::next(block_address count)
{
	current_index_ = min(current_index_ + count, nr_blocks_);

	if (current_index_ + cache_blocks_ < nr_blocks_)
		cache_->prefetch(current_index_ + cache_blocks_);

	return !eof();
}

bool
cache_stream::eof() const
{
	return current_index_ >= nr_blocks_;
}

chunk const &
cache_stream::get()
{
	chunk_wrapper *w = new chunk_wrapper(*this);
	return w->c_;
}

void
cache_stream::put(chunk const &c)
{
	chunk_wrapper *w = container_of(const_cast<chunk *>(&c), chunk_wrapper, c_);
	delete w;
}

cache_stream::chunk_wrapper::chunk_wrapper(cache_stream &parent)
	: block_(parent.cache_->get(parent.current_index_, 0, parent.v_))
{
	c_.offset_ = parent.current_index_ * parent.block_size_;
	c_.len_ = parent.block_size_;
	c_.mem_.begin = static_cast<uint8_t *>(block_.get_data());
	c_.mem_.end = c_.mem_.begin + parent.block_size_;
}

//----------------------------------------------------------------

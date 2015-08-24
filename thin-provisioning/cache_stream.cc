#include "thin-provisioning/cache_stream.h"
#include "persistent-data/file_utils.h"

using namespace thin_provisioning;
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
	load(0);
	for (block_address i = 1; i < min(cache_blocks_, nr_blocks_); i++)
		cache_->prefetch(i);
}

block_address
cache_stream::nr_chunks() const
{
	return nr_blocks_;
}

void
cache_stream::rewind()
{
	load(0);
}

bool
cache_stream::advance(block_address count)
{
	if (current_index_ + count >= nr_blocks_)
		return false;

	current_index_ += count;

	load(current_index_);
	return true;
}

block_address
cache_stream::index() const
{
	return current_index_;
}

chunk const &
cache_stream::get() const
{
	return current_chunk_;
}

void
cache_stream::load(block_address b)
{
	current_index_ = b;
	current_block_ = cache_->get(current_index_, 0, v_);

	current_chunk_.offset_sectors_ = (b * block_size_) / 512;
	current_chunk_.mem_.clear();
	current_chunk_.mem_.push_back(mem(static_cast<uint8_t *>(current_block_.get_data()),
					  static_cast<uint8_t *>(current_block_.get_data()) + block_size_));

	if (current_index_ + cache_blocks_ < nr_blocks_)
		cache_->prefetch(current_index_ + cache_blocks_);
}

//----------------------------------------------------------------

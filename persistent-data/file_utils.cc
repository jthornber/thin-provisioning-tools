#include "persistent-data/math_utils.h"
#include "persistent-data/file_utils.h"
#include "persistent-data/space-maps/core.h"

#include <linux/fs.h>
#include <sys/ioctl.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <sstream>
#include <unistd.h>

using namespace base;
using namespace bcache;
using namespace persistent_data;
using namespace std;

//----------------------------------------------------------------

persistent_data::block_address
persistent_data::get_nr_blocks(std::string const &path, sector_t block_size)
{
	return div_down<block_address>(file_utils::get_file_length(path),
				       block_size);
}

block_address
persistent_data::get_nr_metadata_blocks(std::string const &path)
{
	return get_nr_blocks(path, MD_BLOCK_SIZE);
}

persistent_data::block_manager<>::ptr
persistent_data::open_bm(std::string const &dev_path, block_manager<>::mode m, bool excl)
{
	block_address nr_blocks = get_nr_metadata_blocks(dev_path);
	return block_manager<>::ptr(new block_manager<>(dev_path, nr_blocks, 1, m, excl));
}

block_manager<>::ptr
persistent_data::open_bm(std::string const &path) {
	block_address nr_blocks = get_nr_metadata_blocks(path);
	block_manager<>::mode m = block_manager<>::READ_ONLY;
	return block_manager<>::ptr(new block_manager<>(path, nr_blocks, 1, m));
}

transaction_manager::ptr
persistent_data::open_tm(block_manager<>::ptr bm, block_address superblock_location) {
	auto nr_blocks = bm->get_nr_blocks();
	if (!nr_blocks)
		throw runtime_error("Metadata is not large enough for superblock.");

	space_map::ptr sm(new core_map(nr_blocks));
	sm->inc(superblock_location);
	transaction_manager::ptr tm(new transaction_manager(bm, sm));
	return tm;
}

//----------------------------------------------------------------

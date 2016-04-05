#include "persistent-data/math_utils.h"
#include "persistent-data/file_utils.h"

#include <linux/fs.h>
#include <sys/ioctl.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>

using namespace base;
using namespace bcache;
using namespace persistent_data;

//----------------------------------------------------------------

persistent_data::block_address
persistent_data::get_nr_blocks(string const &path, sector_t block_size)
{
	using namespace persistent_data;

	struct stat info;
	block_address nr_blocks;

	int r = ::stat(path.c_str(), &info);
	if (r)
		throw runtime_error("Couldn't stat dev path " + path + ": " +
			strerror(errno));

	if (S_ISREG(info.st_mode) && info.st_size)
		nr_blocks = div_down<block_address>(info.st_size, block_size);

	else if (S_ISBLK(info.st_mode)) {
		// To get the size of a block device we need to
		// open it, and then make an ioctl call.
		int fd = ::open(path.c_str(), O_RDONLY);
		if (fd < 0)
			throw runtime_error("couldn't open block device to ascertain size");

		r = ::ioctl(fd, BLKGETSIZE64, &nr_blocks);
		if (r) {
			::close(fd);
			throw runtime_error("ioctl BLKGETSIZE64 failed");
		}
		::close(fd);
		nr_blocks = div_down<block_address>(nr_blocks, block_size);
	} else
		// FIXME: needs a better message
		throw runtime_error("bad path");

	return nr_blocks;
}

block_address
persistent_data::get_nr_metadata_blocks(string const &path)
{
	return get_nr_blocks(path, MD_BLOCK_SIZE);
}

persistent_data::block_manager<>::ptr
persistent_data::open_bm(std::string const &dev_path, block_manager<>::mode m, bool excl)
{
	block_address nr_blocks = get_nr_metadata_blocks(dev_path);
	return block_manager<>::ptr(new block_manager<>(dev_path, nr_blocks, 1, m, excl));
}

void
persistent_data::check_file_exists(string const &file) {
	struct stat info;
	int r = ::stat(file.c_str(), &info);
	if (r)
		throw runtime_error("Couldn't stat file");

	if (!S_ISREG(info.st_mode))
		throw runtime_error("Not a regular file");
}

//----------------------------------------------------------------

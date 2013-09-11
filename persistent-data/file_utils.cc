#include "persistent-data/math_utils.h"
#include "persistent-data/file_utils.h"

#include <linux/fs.h>
#include <sys/ioctl.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>

using namespace base;

//----------------------------------------------------------------

persistent_data::block_address
persistent_data::get_nr_blocks(string const &path)
{
	using namespace persistent_data;

	struct stat info;
	block_address nr_blocks;

	int r = ::stat(path.c_str(), &info);
	if (r)
		throw runtime_error("Couldn't stat dev path");

	if (S_ISREG(info.st_mode) && info.st_size)
		nr_blocks = div_up<block_address>(info.st_size, MD_BLOCK_SIZE);

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
		nr_blocks = div_down<block_address>(nr_blocks, MD_BLOCK_SIZE);
	} else
		// FIXME: needs a better message
		throw runtime_error("bad path");

	return nr_blocks;
}

persistent_data::block_manager<>::ptr
persistent_data::open_bm(std::string const &dev_path, block_io<>::mode m)
{
	block_address nr_blocks = get_nr_blocks(dev_path);
	return block_manager<>::ptr(new block_manager<>(dev_path, nr_blocks, 1, m));
}

//----------------------------------------------------------------

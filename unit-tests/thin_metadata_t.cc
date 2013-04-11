#include "gmock/gmock.h"

#include "thin-provisioning/metadata.h"

#include <errno.h>
#include <linux/fs.h>
#include <sys/ioctl.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>

using namespace std;
using namespace persistent_data;
using namespace testing;
using namespace thin_provisioning;

//----------------------------------------------------------------

namespace {
	// FIXME: duplication with block.tcc, factor out a file_utils unit
	void rm_f(string path) {
		struct stat info;
		int r = ::stat(path.c_str(), &info);

		if (r) {
			if (errno == ENOENT)
				return;

			else {
				cerr << "errno == " << errno << endl;
				throw runtime_error("stat failed");
			}
		}

		if (!S_ISREG(info.st_mode))
			throw runtime_error("path isn't a file");

		::unlink(path.c_str());
	}

	void create_sized_file(string const &path, uint64_t file_size) {
		int fd = ::open(path.c_str(), O_CREAT | O_TRUNC | O_RDWR, 0666);
		if (fd < 0)
			throw runtime_error("open_file failed");

		int r = ::lseek(fd, file_size - 1, SEEK_SET);
		if (r < 0)
			throw runtime_error("lseek failed");

		char c = '\0';
		r = ::write(fd, &c, 1);
		if (r < 0)
			throw runtime_error("::write failed");

		if (r != 1)
			throw runtime_error("insufficient bytes written");

		::close(fd);
	}
}

//----------------------------------------------------------------

TEST(ThinMetadataTests, create)
{
	string path("./metadata.bin");
	rm_f(path);
	create_sized_file(path, 4096 * 1024);
	metadata::ptr md(new metadata(path, metadata::CREATE, 128, 102400));
}

//----------------------------------------------------------------

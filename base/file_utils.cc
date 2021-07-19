#include "base/file_utils.h"

#include "base/error_string.h"

#include <sstream>
#include <stdexcept>
#include <linux/fs.h>
#include <sys/ioctl.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <string.h>

//----------------------------------------------------------------

// FIXME: give this namespace a name
namespace {
	using namespace std;

	int const DEFAULT_MODE = 0666;
	int const OPEN_FLAGS = O_DIRECT;

	// FIXME: introduce a new exception for this, or at least lift this
	// to exception.h
	void syscall_failed(char const *call) {
		ostringstream out;
		out << "syscall '" << call << "' failed: " << base::error_string(errno);
		throw runtime_error(out.str());
	}

	void syscall_failed(string const &call, string const &message)
	{
		ostringstream out;
		out << "syscall '" << call << "' failed: " << base::error_string(errno) << "\n"
		    << message;
		throw runtime_error(out.str());
	}
}

//----------------------------------------------------------------

file_utils::file_descriptor::file_descriptor(string const &path, int flags) {
	fd_ = ::open(path.c_str(), OPEN_FLAGS | flags, DEFAULT_MODE);
	if (fd_ < 0)
		syscall_failed("open",
			       "Note: you cannot run this tool with these options on live metadata.");
}

file_utils::file_descriptor::~file_descriptor() {
	close(fd_);
	fd_ = -1;
}

bool
file_utils::file_exists(string const &path) {
	struct ::stat info;

	int r = ::stat(path.c_str(), &info);
	if (r) {
		if (errno == ENOENT)
			return false;

		syscall_failed("stat");
		return false; // never get here

	} else
		return S_ISREG(info.st_mode) || S_ISBLK(info.st_mode);
}

void
file_utils::check_file_exists(string const &file, bool must_be_regular_file) {
	struct stat info;
	int r = ::stat(file.c_str(), &info);
	if (r) {
		ostringstream msg;
		msg << file << ": " << base::error_string(errno);
		throw runtime_error(msg.str());
	}

	if (!S_ISREG(info.st_mode)) {
		if (must_be_regular_file)
			throw runtime_error("Not a regular file");
		if (!S_ISBLK(info.st_mode))
			throw runtime_error("Not a block device or regular file");
	}
}

file_utils::file_descriptor
file_utils::create_block_file(string const &path, off_t file_size) {
	if (file_exists(path)) {
		ostringstream out;
		out << __FUNCTION__ << ": file '" << path << "' already exists";
		throw runtime_error(out.str());
	}

	file_descriptor fd(path, O_CREAT | O_EXCL | O_RDWR);

	int r = ::ftruncate(fd.fd_, file_size);
	if (r < 0)
		syscall_failed("ftruncate");

	return fd;
}

file_utils::file_descriptor
file_utils::open_block_file(string const &path, off_t min_size, bool writeable, bool excl) {
	if (!file_exists(path)) {
		ostringstream out;
		out << __FUNCTION__ << ": file '" << path << "' doesn't exist";
		throw runtime_error(out.str());
	}

	int flags = writeable ? O_RDWR : O_RDONLY;
	if (excl)
		flags |= O_EXCL;

	return file_descriptor(path, flags);
}

uint64_t
file_utils::get_file_length(string const &file) {
	struct stat info;
	uint64_t nr_bytes;

	int r = ::stat(file.c_str(), &info);
	if (r) {
		ostringstream msg;
		msg << file << ": " << base::error_string(errno);
		throw runtime_error(msg.str());
	}

	if (S_ISREG(info.st_mode))
		// It's okay to cast st_size to a uint64_t value.
		// If LFS is enabled, st_size should not be negative for regular files.
		nr_bytes = static_cast<uint64_t>(info.st_size);
	else if (S_ISBLK(info.st_mode)) {
		// To get the size of a block device we need to
		// open it, and then make an ioctl call.
		int fd = ::open(file.c_str(), O_RDONLY);
		if (fd < 0)
			throw runtime_error("couldn't open block device to ascertain size");

		r = ::ioctl(fd, BLKGETSIZE64, &nr_bytes);
		if (r) {
			::close(fd);
			throw runtime_error("ioctl BLKGETSIZE64 failed");
		}
		::close(fd);
	} else {
		ostringstream msg;
		msg << file << ": " << "Not a block device or regular file";
		throw runtime_error(msg.str());
	}

	return nr_bytes;
}

void
file_utils::zero_superblock(std::string const &path)
{
	char *buffer;
	unsigned const SUPERBLOCK_SIZE = 4096;
	file_descriptor fd = open_block_file(path, SUPERBLOCK_SIZE, true, true);

	buffer = reinterpret_cast<char *>(aligned_alloc(SUPERBLOCK_SIZE, SUPERBLOCK_SIZE));
	if (!buffer)
        	throw runtime_error("out of memory");

	memset(buffer, 0, SUPERBLOCK_SIZE);
	if (::write(fd.fd_, buffer, SUPERBLOCK_SIZE) != SUPERBLOCK_SIZE) {
		free(buffer);
		throw runtime_error("couldn't zero superblock");
	}
	free(buffer);
}

//----------------------------------------------------------------

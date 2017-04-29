#include "base/file_utils.h"

#include "base/error_string.h"

#include <sstream>
#include <stdexcept>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>

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

int
file_utils::open_file(string const &path, int flags) {
	int fd = ::open(path.c_str(), OPEN_FLAGS | flags, DEFAULT_MODE);
	if (fd < 0)
		syscall_failed("open",
			       "Note: you cannot run this tool with these options on live metadata.");

	return fd;
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
file_utils::check_file_exists(string const &file) {
	struct stat info;
	int r = ::stat(file.c_str(), &info);
	if (r)
		throw runtime_error("Couldn't stat file");

	if (!S_ISREG(info.st_mode))
		throw runtime_error("Not a regular file");
}

int
file_utils::create_block_file(string const &path, off_t file_size) {
	if (file_exists(path)) {
		ostringstream out;
		out << __FUNCTION__ << ": file '" << path << "' already exists";
		throw runtime_error(out.str());
	}

	int fd = open_file(path, O_CREAT | O_EXCL | O_RDWR);

	int r = ::ftruncate(fd, file_size);
	if (r < 0)
		syscall_failed("ftruncate");

	return fd;
}

int
file_utils::open_block_file(string const &path, off_t min_size, bool writeable, bool excl) {
	if (!file_exists(path)) {
		ostringstream out;
		out << __FUNCTION__ << ": file '" << path << "' doesn't exist";
		throw runtime_error(out.str());
	}

	int flags = writeable ? O_RDWR : O_RDONLY;
	if (excl)
		flags |= O_EXCL;

	return open_file(path, flags);
}

size_t
file_utils::get_file_length(string const &file) {
	struct stat info;
	int r;

	r = ::stat(file.c_str(), &info);
	if (r)
		throw runtime_error("Couldn't stat path");

	return info.st_size;
}

//----------------------------------------------------------------

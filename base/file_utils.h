#ifndef BASE_FILE_UTILS_H
#define BASE_FILE_UTILS_H

#include <string>
#include <sys/types.h>

//----------------------------------------------------------------

namespace file_utils {
	int open_file(std::string const &path, int flags);
	bool file_exists(std::string const &path);
	void check_file_exists(std::string const &file);
	int create_block_file(std::string const &path, off_t file_size);
	int open_block_file(std::string const &path, off_t min_size, bool writeable, bool excl = true);
	size_t get_file_length(std::string const &file);
}

//----------------------------------------------------------------

#endif

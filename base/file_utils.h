#ifndef BASE_FILE_UTILS_H
#define BASE_FILE_UTILS_H

#include <string>
#include <sys/types.h>
#include <stdint.h>

//----------------------------------------------------------------

namespace file_utils {
	struct file_descriptor {
		// file_descriptor is movable but not copyable
		file_descriptor(file_descriptor &&) = default;
		file_descriptor& operator=(file_descriptor &&) = default;
		file_descriptor(std::string const &path, int flags);
		virtual ~file_descriptor();

		int fd_;
	};

	bool file_exists(std::string const &path);
	void check_file_exists(std::string const &file, bool must_be_regular_file = true);
	file_descriptor create_block_file(std::string const &path, off_t file_size);
	file_descriptor open_block_file(std::string const &path, off_t min_size, bool writeable, bool excl = true);
	uint64_t get_file_length(std::string const &file);
	void zero_superblock(std::string const &path);
}

//----------------------------------------------------------------

#endif

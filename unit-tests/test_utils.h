// Copyright (C) 2013 Red Hat, Inc. All rights reserved.
//
// This file is part of the thin-provisioning-tools source.
//
// thin-provisioning-tools is free software: you can redistribute it
// and/or modify it under the terms of the GNU General Public License
// as published by the Free Software Foundation, either version 3 of
// the License, or (at your option) any later version.
//
// thin-provisioning-tools is distributed in the hope that it will be
// useful, but WITHOUT ANY WARRANTY; without even the implied warranty
// of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License along
// with thin-provisioning-tools.  If not, see
// <http://www.gnu.org/licenses/>.

#include "persistent-data/block.h"
#include "persistent-data/transaction_manager.h"

#include <linux/limits.h>

//----------------------------------------------------------------

namespace test {
	using namespace persistent_data;

	unsigned const MAX_HELD_LOCKS = 16;

	template <uint32_t BlockSize>
	typename block_manager<BlockSize>::ptr
	create_bm(block_address nr = 1024) {
		string const path("./test.data");
		int r = system("rm -f ./test.data");
		if (r < 0)
			throw runtime_error("couldn't rm -f ./test.data");

		return typename block_manager<BlockSize>::ptr(
			new block_manager<BlockSize>(path, nr, MAX_HELD_LOCKS,
						     block_manager<BlockSize>::CREATE));
	}

	// Don't use this to update the metadata.
	transaction_manager::ptr open_temporary_tm(block_manager<>::ptr bm);

	void zero_block(block_manager<>::ptr bm, block_address b);

	//--------------------------------

	class with_directory {
	public:
		with_directory(std::string const &path)
			: old_path_(pwd()) {
			chdir(path);
		}

		~with_directory() {
			chdir(old_path_);
		}

	private:
		std::string pwd() const {
			char buffer[PATH_MAX];
			char *ptr = getcwd(buffer, sizeof(buffer));
			if (!ptr) {
				// FIXME: still need a standard syscall failed exception
				throw std::runtime_error("getcwd failed");
			}

			return ptr;
		}

		void chdir(std::string const &path) {
			int r = ::chdir(path.c_str());
			if (r < 0)
				throw std::runtime_error("chdir failed");
		}

		std::string old_path_;
		std::string new_path_;
	};

	class with_temp_directory {
	public:
		with_temp_directory() {
			std::string name("./tmp");

			rm_rf(name);
			mkdir(name);

			dir_.reset(new with_directory(name));
		}

	private:
		void rm_rf(std::string const &name) {
			std::string cmd("rm -rf ");
			cmd += name;
			system(cmd);
		}

		void mkdir(std::string const &name) {
			std::string cmd("mkdir ");
			cmd += name;
			system(cmd);
		}

		void system(std::string const &cmd) {
			int r = ::system(cmd.c_str());
			if (r < 0)
				throw std::runtime_error("system failed");
		}

		std::auto_ptr<with_directory> dir_;
	};

	//--------------------------------

	template <typename T>
	std::ostream &operator <<(std::ostream &out, boost::optional<T> const &maybe) {
		if (maybe)
			out << "Just [" << *maybe << "]";
		else
			out << "Nothing";

		return out;
	}
}

//----------------------------------------------------------------

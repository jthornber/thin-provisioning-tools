// Copyright (C) 2016 Red Hat, Inc. All rights reserved.
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

#include "gmock/gmock.h"
#include "block-cache/copier.h"
#include "test_utils.h"


#include <fcntl.h>

using namespace boost;
using namespace std;
using namespace test;
using namespace testing;

//----------------------------------------------------------------

namespace {
	class temp_file {
	public:
		temp_file(string const &name_base, unsigned meg_size)
			: path_(gen_path(name_base)) {

			int fd = ::open(path_.c_str(), O_CREAT | O_RDWR, 0666);
			if (fd < 0)
				throw runtime_error("couldn't open file");

			if (::fallocate(fd, 0, 0, 1024 * 1024 * meg_size))
				throw runtime_error("couldn't fallocate");

			::close(fd);
		}

		~temp_file() {
			::unlink(path_.c_str());
		}

		string const &get_path() const {
			return path_;
		}

	private:
		static string gen_path(string const &base) {
			return string("./") + base + string(".tmp");
		}

		string path_;
	};

	class CopierTests : public Test {
	public:
		CopierTests()
			: src_file_("copy_src", 32),
			  dest_file_("copy_dest", 32),
			  copier_(src_file_.get_path(),
				  dest_file_.get_path(),
				  64, 1 * 1024 * 1024) {
		}

		copier &get_copier() {
			return copier_;
		}

	private:
		temp_file src_file_;
		temp_file dest_file_;

		copier copier_;
	};
}

//----------------------------------------------------------------

TEST_F(CopierTests, empty_test)
{
	// Copy first block
	copy_op op1(0, 1, 0);
	get_copier().issue(op1);
	auto mop = get_copier().wait();

	if (mop) {
		cerr << "op completed\n";
	} else {
		cerr << "no op completed\n";
	}
}

//----------------------------------------------------------------

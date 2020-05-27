#include "test_utils.h"

#include "persistent-data/space-maps/core.h"

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

using namespace persistent_data;
using namespace std;
using namespace test;

//----------------------------------------------------------------

void test::zero_block(block_manager::ptr bm, block_address b)
{
	block_manager::write_ref wr = bm->write_lock(b);
	memset(wr.data(), 0, 4096);
}

transaction_manager::ptr
test::open_temporary_tm(block_manager::ptr bm)
{
	space_map::ptr sm{create_core_map(bm->get_nr_blocks())};
	transaction_manager::ptr tm(new transaction_manager(bm, sm));
	return tm;
}

//----------------------------------------------------------------

temp_file::temp_file(string const &name_base, unsigned meg_size)
	: path_(gen_path(name_base))
{
	int fd = ::open(path_.c_str(), O_CREAT | O_RDWR, 0666);
	if (fd < 0)
		throw runtime_error("couldn't open file");

	if (::fallocate(fd, 0, 0, 1024 * 1024 * meg_size))
		throw runtime_error("couldn't fallocate");

	::close(fd);
}

temp_file::~temp_file()
{
//	::unlink(path_.c_str());
}

string const &
temp_file::get_path() const
{
	return path_;
}

string
temp_file::gen_path(string const &base)
{
	return string("./") + base + string(".tmp");
}

//----------------------------------------------------------------

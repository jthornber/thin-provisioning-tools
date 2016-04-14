#include "block-cache/copier.h"

#include <stdexcept>

using namespace bcache;
using namespace boost;
using namespace std;

//----------------------------------------------------------------

copier::copier(string const &src, string const &dest,
	       sector_t block_size, size_t mem)
	: pool_(block_size, mem),
	  block_size_(block_size),
	  nr_blocks_(mem / block_size),
	  engine_(nr_blocks_),
	  src_handle_(engine_.open_file(src, io_engine::READ_ONLY)),
	  dest_handle_(engine_.open_file(dest, io_engine::READ_WRITE)),
	  genkey_count_(0)
{
}

void
copier::issue(copy_op const &op)
{
	auto data = pool_.alloc();
	if (!data) {
		wait_();
		data = pool_.alloc();

		if (!data)
			// Shouldn't get here
			throw runtime_error("couldn't allocate buffer");
	}

	copy_job job(op, *data);
	job.op.read_complete = job.op.write_complete = false;
	unsigned key = genkey(); // used as context for the io_engine

	engine_.issue_io(src_handle_,
			 io_engine::READ,
			 to_sector(op.src_b),
			 to_sector(op.src_e),
			 *data,
			 key);
	jobs_.insert(make_pair(key, job));
}

unsigned
copier::nr_pending() const
{
	return jobs_.size() + complete_.size();
}

boost::optional<copy_op>
copier::wait()
{
	while (complete_.empty() && !jobs_.empty())
		wait_();

	if (complete_.empty())
		return optional<copy_op>();

	else {
		auto op = complete_.front();
		complete_.pop_front();
		return optional<copy_op>(op);
	}
}

void
copier::wait_()
{
	auto p = engine_.wait();
	auto it = jobs_.find(p.second);
	if (it == jobs_.end())
		throw runtime_error("Internal error.  Lost track of copy job.");

	copy_job j = it->second;
	if (!p.first) {
		// IO was unsuccessful
		jobs_.erase(it);
		complete(j);
		return;
	}

	// IO was successful
	if (!j.op.read_complete) {
		j.op.read_complete = true;
		engine_.issue_io(dest_handle_,
				 io_engine::WRITE,
				 to_sector(j.op.dest_b),
				 to_sector(j.op.dest_b + (j.op.src_e - j.op.src_b)),
				 j.data,
				 it->first);

	} else {
		j.op.write_complete = true;
		jobs_.erase(it);
		complete(j);
	}
}

void
copier::complete(copy_job const &j)
{
	pool_.free(j.data);
	complete_.push_back(j.op);
}

sector_t
copier::to_sector(block_address b) const
{
	return b * block_size_;
}

unsigned
copier::genkey()
{
	return genkey_count_++;
}

//----------------------------------------------------------------

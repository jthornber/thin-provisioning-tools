#include "block-cache/copier.h"

#include <stdexcept>

using namespace bcache;
using namespace boost;
using namespace std;

//----------------------------------------------------------------

copier::copier(io_engine &engine,
	       string const &src, string const &dest,
	       sector_t block_size, size_t mem)
	: pool_(block_size * 512, mem),
	  block_size_(block_size),
	  nr_blocks_(mem / block_size),
	  engine_(engine),
	  src_handle_(engine_.open_file(src, io_engine::READ_ONLY)),
	  dest_handle_(engine_.open_file(dest, io_engine::READ_WRITE)),
	  genkey_count_(0)
{
}

copier::~copier()
{
	engine_.close_file(src_handle_);
	engine_.close_file(dest_handle_);
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

	copy_job job(op, data);
	job.op.read_complete = job.op.write_complete = false;
	unsigned key = genkey(); // used as context for the io_engine

	auto r = engine_.issue_io(src_handle_,
				  io_engine::READ,
				  to_sector(op.src_b),
				  to_sector(op.src_e),
				  data,
				  key);

	if (r)
		jobs_.insert(make_pair(key, job));
	else
		complete(job);
}

unsigned
copier::nr_pending() const
{
	return jobs_.size() + complete_.size();
}

boost::optional<copy_op>
copier::wait()
{
	while (!jobs_.empty() && complete_.empty())
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

	copy_job &j = it->second;
	if (!p.first) {
		// IO was unsuccessful
		complete(j);
		jobs_.erase(it);
		return;
	}

	// IO was successful
	if (!j.op.read_complete) {
		j.op.read_complete = true;
		if (!engine_.issue_io(dest_handle_,
				      io_engine::WRITE,
				      to_sector(j.op.dest_b),
				      to_sector(j.op.dest_b + (j.op.src_e - j.op.src_b)),
				      j.data,
				      it->first)) {
			complete(j);
			jobs_.erase(it);
		}

	} else {
		j.op.write_complete = true;
		complete(j);
		jobs_.erase(it);
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

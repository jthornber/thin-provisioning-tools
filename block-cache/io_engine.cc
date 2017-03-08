#include "base/container_of.h"
#include "block-cache/io_engine.h"

#include <errno.h>
#include <fcntl.h>
#include <sstream>
#include <stdexcept>
#include <sys/stat.h>
#include <sys/types.h>

using namespace bcache;
using namespace boost;
using namespace std;

//----------------------------------------------------------------

control_block_set::control_block_set(unsigned nr)
	: cbs_(nr)
{
	for (auto i = 0u; i < nr; i++)
		free_cbs_.insert(i);
}

iocb *
control_block_set::alloc(unsigned context)
{
	if (free_cbs_.empty())
		return nullptr;

	auto it = free_cbs_.begin();

	cblock &cb = cbs_[*it];
	cb.context = context;
	free_cbs_.erase(it);

	return &cb.cb;
}

void
control_block_set::free(iocb *cb)
{
	cblock *b = base::container_of(cb, &cblock::cb);
	unsigned index = b - &cbs_[0];
	free_cbs_.insert(index);
}

unsigned
control_block_set::context(iocb *cb) const
{
	cblock *b = base::container_of(cb, &cblock::cb);
	return b->context;
}

//----------------------------------------------------------------

aio_engine::aio_engine(unsigned max_io)
	: aio_context_(0),
	  cbs_(max_io)
{
	int r = io_setup(max_io, &aio_context_);
	if (r < 0)
		throw runtime_error("io_setup failed");
}

aio_engine::~aio_engine()
{
	io_destroy(aio_context_);
}

aio_engine::handle
aio_engine::open_file(std::string const &path, mode m, sharing s)
{
	int flags = (m == M_READ_ONLY) ? O_RDONLY : O_RDWR;
	if (s == EXCLUSIVE)
		flags |= O_EXCL;
	int fd = ::open(path.c_str(), O_DIRECT | flags);
	if (fd < 0) {
		ostringstream out;
		out << "unable to open '" << path << "'";
		throw runtime_error(out.str());
	}

	descriptors_.push_back(base::unique_fd(fd));

	return static_cast<handle>(fd);
}

void
aio_engine::close_file(handle h)
{
	for (auto it = descriptors_.begin(); it != descriptors_.end(); ++it) {
		unsigned it_h = it->get();
		if (it_h == h) {
			descriptors_.erase(it);
			return;
		}
	}

	ostringstream out;
	out << "unknown descriptor (" << h << ")";
	throw runtime_error(out.str());
}

bool
aio_engine::issue_io(handle h, dir d, sector_t b, sector_t e, void *data, unsigned context)
{
	if (reinterpret_cast<uint64_t>(data) & (PAGE_SIZE - 1))
		throw runtime_error("Data passed to issue_io must be page aligned\n");

	iocb *cb;

	cb = cbs_.alloc(context);
	if (!cb)
		return false;

	memset(cb, 0, sizeof(*cb));

	cb->aio_fildes = static_cast<int>(h);
	cb->u.c.buf = data;
	cb->u.c.offset = b << SECTOR_SHIFT;
	cb->u.c.nbytes = (e - b) << SECTOR_SHIFT;
	cb->aio_lio_opcode = (d == D_READ) ? IO_CMD_PREAD : IO_CMD_PWRITE;

	int r = io_submit(aio_context_, 1, &cb);
	return r == 1;
}

optional<io_engine::wait_result>
aio_engine::wait()
{
	return wait_(NULL);
}

optional<io_engine::wait_result>
aio_engine::wait(unsigned &microsec)
{
	timespec start = micro_to_ts(microsec);
	timespec stop = start;
	auto r = wait_(&stop);
	microsec = ts_to_micro(stop) - microsec;
	return r;
}

boost::optional<io_engine::wait_result>
aio_engine::wait_(timespec *ts)
{
	int r;
	struct io_event event;

	memset(&event, 0, sizeof(event));
	r = io_getevents(aio_context_, 1, 1, &event, ts);
	if (r < 0) {
		std::ostringstream out;
		out << "io_getevents failed: " << r;
		throw std::runtime_error(out.str());
	}

	if (r == 0) {
		return optional<wait_result>();
	}

	iocb *cb = reinterpret_cast<iocb *>(event.obj);
	unsigned context = cbs_.context(cb);

	if (event.res == cb->u.c.nbytes) {
		cbs_.free(cb);
		return optional<wait_result>(make_pair(true, context));

	} else if (static_cast<int>(event.res) < 0) {
		cbs_.free(cb);
		return optional<wait_result>(make_pair(false, context));

	} else {
		cbs_.free(cb);
		return optional<wait_result>(make_pair(false, context));
	}

	// shouldn't get here
	return optional<wait_result>(make_pair(false, 0));
}

struct timespec
aio_engine::micro_to_ts(unsigned micro)
{
	timespec ts;
	ts.tv_sec = micro / 1000000u;
	ts.tv_nsec = (micro % 1000000) * 1000;
	return ts;
}

unsigned
aio_engine::ts_to_micro(timespec const &ts)
{
	unsigned micro = ts.tv_sec * 1000000;
	micro += ts.tv_nsec / 1000;
	return micro;
}

//----------------------------------------------------------------

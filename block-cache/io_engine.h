#ifndef BLOCK_CACHE_IO_ENGINE_H
#define BLOCK_CACHE_IO_ENGINE_H

#include "base/unique_handle.h"

#include <boost/optional.hpp>
#include <ctype.h>
#include <set>
#include <string>
#include <libaio.h>
#include <vector>

//----------------------------------------------------------------

namespace bcache {
	using sector_t = uint64_t;

	unsigned const SECTOR_SHIFT = 9;
	unsigned const PAGE_SIZE = 4096;

	// Virtual base class to aid unit testing
	class io_engine {
	public:
		enum mode {
			M_READ_ONLY,
			M_READ_WRITE
		};

		enum dir {
			D_READ,
			D_WRITE
		};

		enum sharing {
			EXCLUSIVE,
			SHARED
		};

		io_engine() {}
		virtual ~io_engine() {}

		using handle = unsigned;

		virtual handle open_file(std::string const &path, mode m, sharing s = EXCLUSIVE) = 0;
		virtual void close_file(handle h) = 0;

		// returns false if there are insufficient resources to
		// queue the IO
		virtual bool issue_io(handle h, dir d, sector_t b, sector_t e, void *data, unsigned context) = 0;

		// returns (success, context)
		using wait_result = std::pair<bool, unsigned>;
		virtual boost::optional<wait_result> wait() = 0;
		virtual boost::optional<wait_result> wait(unsigned &microsec) = 0;

	private:
		io_engine(io_engine const &) = delete;
		io_engine &operator =(io_engine const &) = delete;
	};

	//--------------------------------

	class control_block_set {
	public:
		control_block_set(unsigned nr);

		iocb *alloc(unsigned context);
		void free(iocb *);

		unsigned context(iocb *) const;

	private:
		struct cblock {
			unsigned context;
			struct iocb cb;
		};

		std::set<unsigned> free_cbs_;
		std::vector<cblock> cbs_;
	};

	//----------------

	class aio_engine : public io_engine {
	public:
		// max_io is the maximum nr of concurrent ios expected
		aio_engine(unsigned max_io);
		~aio_engine();

		using handle = unsigned;

		virtual handle open_file(std::string const &path, mode m, sharing s = EXCLUSIVE);
		virtual void close_file(handle h);

		// Returns false if queueing the io failed
		virtual bool issue_io(handle h, dir d, sector_t b, sector_t e, void *data, unsigned context);

		virtual boost::optional<wait_result> wait();
		virtual boost::optional<wait_result> wait(unsigned &microsec);

	private:
		static struct timespec micro_to_ts(unsigned micro);
		static unsigned ts_to_micro(timespec const &ts);
		boost::optional<io_engine::wait_result> wait_(timespec *ts);

		std::list<base::unique_fd> descriptors_;

		io_context_t aio_context_;
		control_block_set cbs_;

		aio_engine(io_engine const &) = delete;
		aio_engine &operator =(io_engine const &) = delete;
	};
}

//----------------------------------------------------------------

#endif

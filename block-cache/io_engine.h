#ifndef BLOCK_CACHE_IO_ENGINE_H
#define BLOCK_CACHE_IO_ENGINE_H

#include "base/unique_handle.h"

#include <boost/optional.hpp>
#include <ctype.h>
#include <libaio.h>
#include <set>
#include <string>

//----------------------------------------------------------------

namespace bcache {
	using sector_t = uint64_t;

	//----------------

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

	class io_engine {
	public:
		enum mode {
			READ_ONLY,
			READ_WRITE
		};

		enum dir {
			READ,
			WRITE
		};

		// max_io is the maximum nr of concurrent ios expected
		io_engine(unsigned max_io);
		~io_engine();

		using handle = unsigned;

		handle open_file(std::string const &path, mode m);
		void close_file(handle h);

		// returns false if there are insufficient resources to
		// queue the IO
		bool issue_io(handle h, dir d, sector_t b, sector_t e, void *data, unsigned context);

		// returns (success, context)
		std::pair<bool, unsigned> wait();

	private:
		std::list<base::unique_fd> descriptors_;

		io_context_t aio_context_;
		control_block_set cbs_;
		std::vector<io_event> events_;

		io_engine(io_engine const &) = delete;
		io_engine &operator =(io_engine const &) = delete;
	};
}

//----------------------------------------------------------------

#endif

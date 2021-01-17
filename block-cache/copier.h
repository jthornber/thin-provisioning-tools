#ifndef BLOCK_CACHE_COPIER_H
#define BLOCK_CACHE_COPIER_H

#include "block-cache/io_engine.h"
#include "block-cache/mem_pool.h"

#include <string>
#include <list>
#include <map>

//----------------------------------------------------------------

namespace bcache {
	using block_address = uint64_t;

	struct copy_op {
		copy_op()
			: src_b(0),
			  src_e(0),
			  dest_b(0),
			  read_complete(false),
			  write_complete(false) {
		}

		copy_op(block_address src_b_,
			block_address src_e_,
			block_address dest_b_)
			: src_b(src_b_),
			  src_e(src_e_),
			  dest_b(dest_b_),
			  read_complete(false),
			  write_complete(false) {
		}

		bool operator <(copy_op const &rhs) const {
			return dest_b < rhs.dest_b;
		}

		bool success() const {
			return read_complete && write_complete;
		}

		block_address src_b, src_e;
		block_address dest_b;

		bool read_complete;
		bool write_complete;
	};

	class copy_job {
	public:
		copy_job(copy_op const &op_, void *data_)
			: op(op_), data(data_) {
		}

		copy_op op;
		void *data;
	};

	class copier {
	public:
		copier(io_engine &engine,
		       std::string const &src, std::string const &dest,
		       sector_t block_size, size_t mem,
		       sector_t src_offset, sector_t dest_offset);
		~copier();

		sector_t get_block_size() const {
			return block_size_;
		}

		sector_t get_src_offset() const {
			return src_offset_;
		}

		sector_t get_dest_offset() const {
			return dest_offset_;
		}

		// Blocks if out of memory.
		void issue(copy_op const &op);

		unsigned nr_pending() const;
		boost::optional<copy_op> wait();
		boost::optional<copy_op> wait(unsigned &micro);

	private:
		bool pending() const;
		bool wait_successful(io_engine::wait_result const &p);
		boost::optional<copy_op> wait_complete();
		void wait_(unsigned &micro);
		void wait_();
		void complete(copy_job const &j);

		sector_t to_src_sector(block_address b) const;
		sector_t to_dest_sector(block_address b) const;
		unsigned genkey();

		mempool pool_;
		sector_t block_size_;
		io_engine &engine_;
		io_engine::handle src_handle_;
		io_engine::handle dest_handle_;
		sector_t src_offset_;
		sector_t dest_offset_;
		unsigned genkey_count_;

		using job_map = std::map<unsigned, copy_job>;
		using op_list = std::list<copy_op>;
		job_map jobs_;
		op_list complete_;
	};
}

//----------------------------------------------------------------

#endif

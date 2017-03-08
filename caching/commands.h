#ifndef CACHING_COMMANDS_H
#define CACHING_COMMANDS_H

#include "base/application.h"
#include "boost/optional.hpp"

//----------------------------------------------------------------

namespace caching {
	class cache_check_cmd : public base::command {
	public:
		cache_check_cmd();
		virtual void usage(std::ostream &out) const;
		virtual int run(int argc, char **argv);
	};

	class cache_dump_cmd : public base::command {
	public:
		cache_dump_cmd();
		virtual void usage(std::ostream &out) const;
		virtual int run(int argc, char **argv);
	};

	class cache_metadata_size_cmd : public base::command {
	public:
		cache_metadata_size_cmd();
		virtual void usage(std::ostream &out) const;
		virtual int run(int argc, char **argv);


	private:
		struct flags {
			flags();

			boost::optional<uint64_t> device_size;
			boost::optional<uint32_t> block_size;
			boost::optional<uint64_t> nr_blocks;
			uint32_t max_hint_width;
		};

		enum parse_result {
			FINISH,
			CONTINUE
		};

		parse_result parse_command_line(std::string const &prog_name, int argc, char **argv, flags &fs);
		uint64_t get_nr_blocks(flags &fs);
		uint64_t meg(uint64_t n);
		uint64_t calc_size(uint64_t nr_blocks, uint32_t max_hint_width);
	};

	class cache_repair_cmd : public base::command {
	public:
		cache_repair_cmd();
		virtual void usage(std::ostream &out) const;
		virtual int run(int argc, char **argv);
	};

	class cache_restore_cmd : public base::command {
	public:
		cache_restore_cmd();
		virtual void usage(std::ostream &out) const;
		virtual int run(int argc, char **argv);
	};

	class cache_writeback_cmd : public base::command {
	public:
		cache_writeback_cmd();
		virtual void usage(std::ostream &out) const;
		virtual int run(int argc, char **argv);
	};

	void register_cache_commands(base::application &app);
}

//----------------------------------------------------------------

#endif

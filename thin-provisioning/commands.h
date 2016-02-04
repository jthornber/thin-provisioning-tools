#ifndef THIN_PROVISIONING_COMMANDS_H
#define THIN_PROVISIONING_COMMANDS_H

#include "base/application.h"
#include "boost/optional.hpp"

//----------------------------------------------------------------

namespace thin_provisioning {
	class thin_check_cmd : public base::command {
	public:
		thin_check_cmd();
		virtual void usage(std::ostream &out) const;
		virtual int run(int argc, char **argv);
	};

	class thin_delta_cmd : public base::command {
	public:
		thin_delta_cmd();
		virtual void usage(std::ostream &out) const;
		virtual int run(int argc, char **argv);
	};

	class thin_dump_cmd : public base::command {
	public:
		thin_dump_cmd();
		virtual void usage(std::ostream &out) const;
		virtual int run(int argc, char **argv);
	};

	class thin_ls_cmd : public base::command {
	public:
		thin_ls_cmd();
		virtual void usage(std::ostream &out) const;
		virtual int run(int argc, char **argv);
	};

	class thin_metadata_size_cmd : public base::command {
	public:
		thin_metadata_size_cmd();
		virtual void usage(std::ostream &out) const;
		virtual int run(int argc, char **argv);
	};

	class thin_restore_cmd : public base::command {
	public:
		thin_restore_cmd();
		virtual void usage(std::ostream &out) const;
		virtual int run(int argc, char **argv);
	};

	class thin_repair_cmd : public base::command {
	public:
		thin_repair_cmd();
		virtual void usage(std::ostream &out) const;
		virtual int run(int argc, char **argv);
	};

	class thin_rmap_cmd : public base::command {
	public:
		thin_rmap_cmd();
		virtual void usage(std::ostream &out) const;
		virtual int run(int argc, char **argv);
	};

	class thin_trim_cmd : public base::command {
	public:
		thin_trim_cmd();
		virtual void usage(std::ostream &out) const;
		virtual int run(int argc, char **argv);
	};

	void register_thin_commands(base::application &app);
}

//----------------------------------------------------------------

#endif

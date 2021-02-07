#ifndef ERA_COMMANDS_H
#define ERA_COMMANDS_H

#include "base/application.h"

//----------------------------------------------------------------

namespace era {
	class era_check_cmd : public base::command {
	public:
		era_check_cmd();
		virtual void usage(std::ostream &out) const;
		virtual int run(int argc, char **argv);
	};

	class era_dump_cmd : public base::command {
	public:
		era_dump_cmd();
		virtual void usage(std::ostream &out) const;
		virtual int run(int argc, char **argv);
	};

	class era_invalidate_cmd : public base::command {
	public:
		era_invalidate_cmd();
		virtual void usage(std::ostream &out) const;
		virtual int run(int argc, char **argv);
	};

	class era_restore_cmd : public base::command {
	public:
		era_restore_cmd();
		virtual void usage(std::ostream &out) const;
		virtual int run(int argc, char **argv);
	};

	//------------------------------------------------------

	class era_debug_cmd : public base::command {
	public:
		era_debug_cmd();

		virtual void usage(std::ostream &out) const;
		virtual int run(int argc, char **argv);
	};

	//------------------------------------------------------

	void register_era_commands(base::application &app);
}

//----------------------------------------------------------------

#endif

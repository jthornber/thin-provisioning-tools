#ifndef CRUNCHER_COMMANDS_H
#define CRUNCHER_COMMANDS_H

#include "base/application.h"

#include <string>

//----------------------------------------------------------------

namespace cruncher {
	class crunch_cmd : public base::command {
	public:
		crunch_cmd();
		virtual void usage(std::ostream &out) const;
		virtual int run(int argc, char **argv);

	private:
		struct flags {
			std::string source;
		};

		int crunch(flags const &fs);
	};

	void register_cruncher_commands(base::application &app);
}

//----------------------------------------------------------------

#endif

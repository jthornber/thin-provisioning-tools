#ifndef BASE_APPLICATION_H
#define BASE_APPLICATION_H

#include <iostream>
#include <list>
#include <string>
#include <stdexcept>

//----------------------------------------------------------------

namespace base {
	class command {
	public:
		typedef int (*cmd_fn)(int, char **);

		command(std::string const &name, cmd_fn fn)
			: name_(name),
			  fn_(fn) {
		}

		std::string const &get_name() const {
			return name_;
		}

		int run(int argc, char **argv) const {
			return fn_(argc, argv);
		}

	private:
		std::string name_;
		cmd_fn fn_;
	};

	class application {
	public:
		void add_cmd(command const &c) {
			cmds_.push_back(&c);
		}

		int run(int argc, char **argv);

	private:
		void usage();
		std::string basename(std::string const &path) const;

		std::list<command const *> cmds_;
	};
}

//----------------------------------------------------------------

#endif

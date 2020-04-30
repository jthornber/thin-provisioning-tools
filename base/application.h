#ifndef BASE_APPLICATION_H
#define BASE_APPLICATION_H

#include <iostream>
#include <list>
#include <memory>
#include <string>
#include <stdexcept>
#include <stdint.h>

//----------------------------------------------------------------

namespace base {
	class command {
	public:
		typedef std::shared_ptr<command> ptr;

		command(std::string const &name);
		virtual ~command() {}

		void die(std::string const &msg);
		uint64_t parse_uint64(std::string const &str, std::string const &desc);


		virtual void usage(std::ostream &out) const = 0;
		virtual int run(int argc, char **argv) = 0;

		std::string const &get_name() const {
			return name_;
		}

	private:
		std::string name_;

	};

	class command_old : public command {
	public:
		typedef int (*cmd_fn)(int, char **);

		command_old(std::string const &name, cmd_fn fn)
			: command(name),
			  fn_(fn) {
		}

		int run(int argc, char **argv) {
			return fn_(argc, argv);
		}

	private:
		cmd_fn fn_;
	};

	class application {
	public:
		void add_cmd(command::ptr c) {
			cmds_.push_back(c);
		}

		int run(int argc, char **argv);

	private:
		void usage();
		std::string get_basename(std::string const &path) const;

		std::list<command::ptr> cmds_;
	};
}

//----------------------------------------------------------------

#endif

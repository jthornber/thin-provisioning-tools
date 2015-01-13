#ifndef THINP_TOOLS_DEVICE_MAPPER_INTERFACE_H
#define THINP_TOOLS_DEVICE_MAPPER_INTERFACE_H

#include <string>
#include <stdint.h>
#include <vector>

//----------------------------------------------------------------

namespace dm {
	struct dev_info {
		dev_info(unsigned major, unsigned minor, std::string const &name)
			: major_(major), // with flies in his eyes
			  minor_(minor),
			  name_(name) {
		}

		unsigned major_;
		unsigned minor_;
		std::string name_;
	};

	struct target_info {
		target_info(uint64_t len, std::string const &type, std::string const &args)
			: length_sectors_(len),
			  type_(type),
			  args_(args) {
		}

		uint64_t length_sectors_;
		std::string type_;
		std::string args_;
	};

	//--------------------------------

	class interface;
	class instruction {
	public:
		virtual ~instruction() {}

	protected:
		friend interface;
		virtual void execute(interface &dm) = 0;
	};

	class version_instr : public instruction {
	public:
		version_instr()
			: major_(0),
			  minor_(0),
			  patch_(0) {
		}

		void set_version(unsigned major, unsigned minor, unsigned patch) {
			major_ = major;
			minor_ = minor;
			patch_ = patch;
		}

		unsigned get_major() const {
			return major_;
		}

		unsigned get_minor() const {
			return minor_;
		}

		unsigned get_patch() const {
			return patch_;
		}

	protected:
		virtual void execute(interface &dm);

	private:
		unsigned major_, minor_, patch_;
	};

	class remove_all_instr : public instruction {
	public:
	protected:
		virtual void execute(interface &dm);
	};

	//--------------------------------

	class list_devices_instr : public instruction {
	public:
		typedef std::vector<dev_info> dev_list;
		dev_list const &get_devices() const {
			return devs_;
		}

		void add_device(unsigned major, unsigned minor, std::string const &name) {
			devs_.push_back(dev_info(major, minor, name));
		}

	protected:
		virtual void execute(interface &dm);

	private:
		dev_list devs_;
	};

	//--------------------------------

	class named_instr : public instruction {
	public:
		named_instr(std::string const &name)
			: instruction(),
			  name_(name) {
		}

		std::string const &get_name() const {
			return name_;
		}

	private:
		std::string name_;
	};

	class create_instr : public named_instr {
	public:
		create_instr(std::string const &name, std::string const &uuid)
			: named_instr(name),
			  uuid_(uuid) {
		}

		std::string const &get_uuid() const {
			return uuid_;
		}

	protected:
		virtual void execute(interface &dm);

	private:
		std::string name_, uuid_;
	};

	class remove_instr : public named_instr {
	public:
		remove_instr(std::string const &name)
			: named_instr(name) {
		}

	protected:
		virtual void execute(interface &dm);
	};

	class suspend_instr : public named_instr {
	public:
		suspend_instr(std::string const &name)
			: named_instr(name) {
		}

	protected:
		virtual void execute(interface &dm);
	};

	class resume_instr : public named_instr {
	public:
		resume_instr(std::string const &name)
			: named_instr(name) {
		}

	protected:
		virtual void execute(interface &dm);
	};

	class clear_instr : public named_instr {
	public:
		clear_instr(std::string const &name)
			: named_instr(name) {
		}

	protected:
		virtual void execute(interface &dm);
	};

	class table_mixin {
	public:
		void add_target(target_info const &ti) {
			targets_.push_back(ti);
		}

		typedef std::vector<target_info> target_list;
		target_list const &get_targets() const {
			return targets_;
		}

	private:
		target_list targets_;
	};

	class load_instr : public named_instr, public table_mixin {
	public:
		load_instr(std::string const &name)
			: named_instr(name) {
		}

	protected:
		virtual void execute(interface &dm);
	};

	class status_instr : public named_instr, public table_mixin {
	public:
		status_instr(std::string const &name)
			: named_instr(name) {
		}

	protected:
		virtual void execute(interface &dm);
	};

	class table_instr : public named_instr, public table_mixin {
	public:
		table_instr(std::string const &name)
			: named_instr(name) {
		}

	protected:
		virtual void execute(interface &dm);
	};

	class info_instr : public named_instr, public table_mixin {
	public:
		info_instr(std::string const &name)
			: named_instr(name) {
		}

	protected:
		virtual void execute(interface &dm);
	};

	class message_instr : public instruction {
	public:
	protected:
		virtual void execute(interface &dm);
	};

	//--------------------------------

	class interface {
	public:
		virtual ~interface() {}

		void execute(instruction &instr);

		virtual void execute(version_instr &instr) = 0;
		virtual void execute(remove_all_instr &instr) = 0;
		virtual void execute(list_devices_instr &instr) = 0;
		virtual void execute(create_instr &instr) = 0;
		virtual void execute(remove_instr &instr) = 0;
		virtual void execute(suspend_instr &instr) = 0;
		virtual void execute(resume_instr &instr) = 0;
		virtual void execute(clear_instr &instr) = 0;
		virtual void execute(load_instr &instr) = 0;
		virtual void execute(status_instr &instr) = 0;
		virtual void execute(table_instr &instr) = 0;
		virtual void execute(info_instr &instr) = 0;
		virtual void execute(message_instr &instr) = 0;
	};
}

//----------------------------------------------------------------

#endif

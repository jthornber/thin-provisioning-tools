#ifndef THINP_TOOLS_DEVICE_MAPPER_INTERFACE_H
#define THINP_TOOLS_DEVICE_MAPPER_INTERFACE_H

#include <string>

//----------------------------------------------------------------

namespace dm {
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
		void set_version(unsigned *version);
		unsigned const *get_version() const {
			return version_;
		}

	protected:
		virtual void execute(interface &dm);

	private:
		unsigned version_[3];
	};

	class remove_all_instr : public instruction {
	public:
	protected:
		virtual void execute(interface &dm);
	};

	class list_devices_instr : public instruction {
	public:
	protected:
		virtual void execute(interface &dm);
	};

	class create_instr : public instruction {
	public:
	protected:
		virtual void execute(interface &dm);
	};

	class remove_instr : public instruction {
	public:
	protected:
		virtual void execute(interface &dm);
	};

	class suspend_instr : public instruction {
	public:
	protected:
		virtual void execute(interface &dm);
	};

	class resume_instr : public instruction {
	public:
	protected:
		virtual void execute(interface &dm);
	};

	class clear_instr : public instruction {
	public:
	protected:
		virtual void execute(interface &dm);
	};

	class load_instr : public instruction {
	public:
	protected:
		virtual void execute(interface &dm);
	};

	class status_instr : public instruction {
	public:
	protected:
		virtual void execute(interface &dm);
	};

	class table_instr : public instruction {
	public:
	protected:
		virtual void execute(interface &dm);
	};

	class info_instr : public instruction {
	public:
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

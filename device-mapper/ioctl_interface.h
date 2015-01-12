#ifndef DEVICE_MAPPER_IOCTL_INTERFACE_H
#define DEVICE_MAPPER_IOCTL_INTERFACE_H

#include "device-mapper/interface.h"

#include <linux/dm-ioctl.h>
#include <vector>

//----------------------------------------------------------------

namespace dm {
	class ioctl_buffer {
	public:
		// The payload size does not include the dm_ioctl control
		// struct, it specifies any additional space that you
		// require.
		ioctl_buffer(size_t payload_size);

		dm_ioctl &get_ctl();
		dm_ioctl const &get_ctl() const;

		size_t payload_size() const;
		void *get_payload();
		void const *get_payload() const;

	private:
		size_t payload_size_;
		std::vector<unsigned char> buffer_;
		dm_ioctl *ctl_;
		void *payload_;

	};

	class ioctl_interface : public interface {
	public:
		ioctl_interface();
		~ioctl_interface();

		void execute(version_instr &instr);
		void execute(remove_all_instr &instr);
		void execute(list_devices_instr &instr);
		void execute(create_instr &instr);
		void execute(remove_instr &instr);
		void execute(suspend_instr &instr);
		void execute(resume_instr &instr);
		void execute(clear_instr &instr);
		void execute(load_instr &instr);
		void execute(status_instr &instr);
		void execute(table_instr &instr);
		void execute(info_instr &instr);
		void execute(message_instr &instr);

	private:
		static int open_control_file();
		void ioctl(int request, ioctl_buffer &buffer);

		int ctl_fd_;
	};
}

//----------------------------------------------------------------

#endif

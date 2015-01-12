#include "ioctl_interface.h"

#include <fcntl.h>
#include <sstream>
#include <stdexcept>
#include <stdio.h>
#include <string.h>
#include <sys/ioctl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

using namespace dm;
using namespace std;

//----------------------------------------------------------------

ioctl_buffer::ioctl_buffer(size_t payload_size)
	: payload_size_(payload_size),
	  buffer_(payload_size + sizeof(dm_ioctl), '\0'),
	  ctl_(reinterpret_cast<dm_ioctl *>(&buffer_[0])),
	  payload_(ctl_ + sizeof(*ctl_))
{
	ctl_->version[0] = DM_VERSION_MAJOR;
	ctl_->version[1] = DM_VERSION_MINOR;
	ctl_->version[2] = DM_VERSION_PATCHLEVEL;
	ctl_->data_size = payload_size_;
	ctl_->data_start = sizeof(*ctl_);
}

dm_ioctl &
ioctl_buffer::get_ctl()
{
	return *ctl_;
}

dm_ioctl const &
ioctl_buffer::get_ctl() const
{
	return *ctl_;
}

size_t
ioctl_buffer::payload_size() const
{
	return payload_size_;
}

void *
ioctl_buffer::get_payload()
{
	return payload_;
}

void const *
ioctl_buffer::get_payload() const
{
	return payload_;
}

//----------------------------------------------------------------

ioctl_interface::ioctl_interface()
	: ctl_fd_(open_control_file())
{
}

ioctl_interface::~ioctl_interface()
{
	::close(ctl_fd_);
}

void
ioctl_interface::execute(version_instr &instr)
{
	ioctl_buffer iob(0);
	ioctl(DM_VERSION, iob);

	dm_ioctl &ctl = iob.get_ctl();
	instr.set_version(ctl.version);
}

void
ioctl_interface::execute(remove_all_instr &instr)
{

}

void
ioctl_interface::execute(list_devices_instr &instr)
{

}

void
ioctl_interface::execute(create_instr &instr)
{

}

void
ioctl_interface::execute(remove_instr &instr)
{

}

void
ioctl_interface::execute(suspend_instr &instr)
{

}

void
ioctl_interface::execute(resume_instr &instr)
{

}

void
ioctl_interface::execute(clear_instr &instr)
{

}

void
ioctl_interface::execute(load_instr &instr)
{

}

void
ioctl_interface::execute(status_instr &instr)
{

}

void
ioctl_interface::execute(table_instr &instr)
{

}

void
ioctl_interface::execute(info_instr &instr)
{
}

void
ioctl_interface::execute(message_instr &instr)
{

}

int
ioctl_interface::open_control_file()
{
	char buffer[1024];
	snprintf(buffer, sizeof(buffer), "/dev/%s/%s", DM_DIR, DM_CONTROL_NODE);
	int fd = open(buffer, O_RDWR | O_EXCL);
	if (fd < 0) {
		ostringstream out;
		out << "couldn't open control device: '"
		    << buffer << "'";
		throw runtime_error(out.str());
	}

	return fd;
}

void
ioctl_interface::ioctl(int request, ioctl_buffer &buffer)
{
	int r = ::ioctl(ctl_fd_, request, &buffer.get_ctl());
	if (r < 0)
		throw runtime_error("dm ioctl call failed");
}

//----------------------------------------------------------------

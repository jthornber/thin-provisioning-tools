#include "ioctl_interface.h"

#include "persistent-data/math_utils.h"

#include <errno.h>
#include <fcntl.h>
#include <memory>
#include <sstream>
#include <stdexcept>
#include <stdio.h>
#include <string.h>
#include <sys/ioctl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include <iostream>

using namespace base;
using namespace dm;
using namespace std;

//----------------------------------------------------------------

namespace {
	template <typename T>
	void copy_string(string const &src, T &dest) {
		if (src.length() > (sizeof(dest) - 1)) {
			ostringstream out;
			out << "copy_string failed, destination too small: '" << src << "'";
			throw runtime_error(out.str());
		}

		memset(dest, 0, sizeof(dest));
		memcpy(dest, src.c_str(), src.length());
	}
}

//----------------------------------------------------------------

ioctl_buffer::ioctl_buffer(size_t payload_size)
	: buffer_(payload_size + sizeof(dm_ioctl), '\0'),
	  ctl_(reinterpret_cast<dm_ioctl *>(&buffer_[0]))
{
	ctl_->version[0] = DM_VERSION_MAJOR;
	ctl_->version[1] = DM_VERSION_MINOR;
	ctl_->version[2] = DM_VERSION_PATCHLEVEL;
	ctl_->data_size = buffer_.size();
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
	return ctl_->data_size;
}

void *
ioctl_buffer::get_payload()
{
	return reinterpret_cast<unsigned char *>(ctl_) + ctl_->data_start;
}

void const *
ioctl_buffer::get_payload() const
{
	return reinterpret_cast<unsigned char *>(ctl_) + ctl_->data_start;
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
	ioctl_buffer iob;
	ioctl(DM_VERSION, iob);

	dm_ioctl &ctl = iob.get_ctl();
	instr.set_version(ctl.version[0], ctl.version[1], ctl.version[2]);
}

void
ioctl_interface::execute(remove_all_instr &instr)
{
	ioctl_buffer iob;
	ioctl(DM_REMOVE_ALL, iob);
}

void
ioctl_interface::execute(list_devices_instr &instr)
{
	// FIXME: can we factor out this payload loop? into a template?
	auto_ptr<ioctl_buffer> iob;
	size_t payload_size = 8192;

	do {
		iob.reset(new ioctl_buffer(payload_size));
		ioctl(DM_LIST_DEVICES, *iob);
		payload_size *= 2;

	} while (iob->get_ctl().flags & DM_BUFFER_FULL_FLAG);

	dm_name_list *nl = reinterpret_cast<dm_name_list *>(iob->get_payload());
	if (nl->dev) {
		// FIXME: ugly loop
		for (;;) {
			instr.add_device(major(nl->dev), minor(nl->dev), nl->name);

			if (!nl->next)
				break;

			nl = reinterpret_cast<dm_name_list *>(reinterpret_cast<unsigned char *>(nl) + nl->next);
		}
	} else
		cerr << "nl->dev is NULL\n";
}

void
ioctl_interface::execute(create_instr &instr)
{
	ioctl_buffer iob;
	copy_string(instr.get_name(), iob.get_ctl().name);
	copy_string(instr.get_uuid(), iob.get_ctl().uuid);
	ioctl(DM_DEV_CREATE, iob);
}

void
ioctl_interface::execute(remove_instr &instr)
{
	ioctl_buffer iob;
	copy_string(instr.get_name(), iob.get_ctl().name);
	ioctl(DM_DEV_REMOVE, iob);
}

void
ioctl_interface::execute(suspend_instr &instr)
{
	ioctl_buffer iob;
	copy_string(instr.get_name(), iob.get_ctl().name);
	iob.get_ctl().flags = DM_SUSPEND_FLAG;
	ioctl(DM_DEV_SUSPEND, iob);
}

void
ioctl_interface::execute(resume_instr &instr)
{
	ioctl_buffer iob;
	copy_string(instr.get_name(), iob.get_ctl().name);
	ioctl(DM_DEV_SUSPEND, iob);
}

void
ioctl_interface::execute(clear_instr &instr)
{
	ioctl_buffer iob;
	copy_string(instr.get_name(), iob.get_ctl().name);
	ioctl(DM_TABLE_CLEAR, iob);
}

void
ioctl_interface::execute(load_instr &instr)
{
	auto_ptr<ioctl_buffer> iob;
	size_t payload_size = 1024;

	// FIXME: add retry with bigger buffer
	{
		iob.reset(new ioctl_buffer(payload_size));
		load_instr::target_list const &targets = instr.get_targets();

		dm_ioctl &ctl = iob->get_ctl();
		copy_string(instr.get_name(), ctl.name);
		ctl.target_count = targets.size();

		dm_target_spec *spec = reinterpret_cast<dm_target_spec *>(iob->get_payload());

		load_instr::target_list::const_iterator it, e = targets.end();
		uint64_t current_sector = 0;
		size_t buffer_remaining = iob->payload_size();

		for (it = targets.begin(); it != e; ++it) {
			size_t space_required = sizeof(*spec) + round_up<unsigned>(it->args_.length() + 1, 8);

			if (buffer_remaining < space_required)
				throw runtime_error("insufficient buffer space for targets"); // FIXME: retry

			spec->sector_start = current_sector;
			spec->length = it->length_sectors_;
			spec->status = 0;
			spec->next = space_required;

			copy_string(it->type_, spec->target_type);

			char *args_dest = reinterpret_cast<char *>(spec + 1);
			memcpy(args_dest, it->args_.c_str(), it->args_.length());
			args_dest[it->args_.length()] = '\0';


			spec = reinterpret_cast<dm_target_spec *>(reinterpret_cast<char *>(spec) + spec->next);
			current_sector += it->length_sectors_;
		}

		ioctl(DM_TABLE_LOAD, *iob);
	}

}

// FIXME: factor out common code
void
ioctl_interface::execute(status_instr &instr)
{
	size_t payload_size = 1024;
	ioctl_buffer iob(payload_size);

	dm_ioctl &ctl = iob.get_ctl();
	copy_string(instr.get_name(), ctl.name);
	ctl.flags = 0;
	ioctl(DM_TABLE_STATUS, iob);

	if (ctl.flags & DM_BUFFER_FULL_FLAG)
		throw runtime_error("ioctl buffer full"); // FIXME: retry

	dm_target_spec *spec = reinterpret_cast<dm_target_spec *>(iob.get_payload());
	cerr << "ctl.target_count = " << ctl.target_count << "\n";
	for (unsigned i = 0; i < ctl.target_count; i++) {
		char *args = reinterpret_cast<char *>(spec + 1);
		target_info ti(spec->length, spec->target_type, args);
		instr.add_target(ti);

		spec = reinterpret_cast<dm_target_spec *>(reinterpret_cast<char *>(spec) + spec->next);
	}
}

void
ioctl_interface::execute(table_instr &instr)
{
	size_t payload_size = 1024;
	ioctl_buffer iob(payload_size);

	dm_ioctl &ctl = iob.get_ctl();
	copy_string(instr.get_name(), ctl.name);
	ctl.flags = DM_STATUS_TABLE_FLAG;
	ioctl(DM_TABLE_STATUS, iob);

	if (ctl.flags & DM_BUFFER_FULL_FLAG)
		throw runtime_error("ioctl buffer full"); // FIXME: retry

	dm_target_spec *spec = reinterpret_cast<dm_target_spec *>(iob.get_payload());
	cerr << "ctl.target_count = " << ctl.target_count << "\n";
	for (unsigned i = 0; i < ctl.target_count; i++) {
		char *args = reinterpret_cast<char *>(spec + 1);
		target_info ti(spec->length, spec->target_type, args);
		instr.add_target(ti);

		spec = reinterpret_cast<dm_target_spec *>(reinterpret_cast<char *>(spec) + spec->next);
	}
}

void
ioctl_interface::execute(info_instr &instr)
{
	throw runtime_error("not implemented");
}

void
ioctl_interface::execute(message_instr &instr)
{
	string const &txt = instr.get_message();
	size_t payload_size = sizeof(dm_target_msg) + txt.length() + 1;
	ioctl_buffer iob(payload_size);

	dm_target_msg *msg = reinterpret_cast<dm_target_msg *>(iob.get_payload());
	copy_string(instr.get_name(), iob.get_ctl().name);
	msg->sector = instr.get_sector();
	memcpy(msg->message, txt.c_str(), txt.length());
	msg->message[txt.length()] = '\0';

	ioctl(DM_TARGET_MSG_CMD, iob);
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
	if (r < 0) {
		ostringstream out;
		char buffer[512], *ptr;

		// GNU specific version of strerror_r
		ptr = strerror_r(errno, buffer, sizeof(buffer));
		out << "dm ioctl call failed: " << ptr;
		throw runtime_error(out.str());
	}
}

//----------------------------------------------------------------

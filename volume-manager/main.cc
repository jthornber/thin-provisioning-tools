#include "device-mapper/ioctl_interface.h"

#include <iostream>

using namespace dm;
using namespace std;

//----------------------------------------------------------------

int main(int argc, char **argv)
{
	ioctl_interface dm;

	{
		version_instr v;
		dm.execute(v);
		cout << "dm version: "
		     << v.get_major() << ", "
		     << v.get_minor() << ", "
		     << v.get_patch() << "\n";
	}

	{
		remove_all_instr i;
		dm.execute(i);
	}

	{
		create_instr i("foo", "lskflsk");
		dm.execute(i);
	}

	{
		load_instr i("foo");
		i.add_target(target_info(1024, "linear", "/dev/sdb 0"));
		i.add_target(target_info(1024, "linear", "/dev/vdd 2048"));
		dm.execute(i);
	}

	{
		resume_instr i("foo");
		dm.execute(i);
	}

	{
		list_devices_instr i;
		dm.execute(i);

		list_devices_instr::dev_list::const_iterator it, e = i.get_devices().end();
		for (it = i.get_devices().begin(); it != e; ++it)
			cerr << "device: " << it->name_ << "(" << it->major_ << ", " << it->minor_ << ")\n";
	}

	{
		table_instr i("foo");
		dm.execute(i);

		table_instr::target_list::const_iterator it, e = i.get_targets().end();
		for (it = i.get_targets().begin(); it != e; ++it) {
			cerr << "target: " << it->length_sectors_ << " " << it->type_ << " " << it->args_ << "\n";
		}
	}

	{
		status_instr i("foo");
		dm.execute(i);

		table_instr::target_list::const_iterator it, e = i.get_targets().end();
		for (it = i.get_targets().begin(); it != e; ++it) {
			cerr << "target: " << it->length_sectors_ << " " << it->type_ << " " << it->args_ << "\n";
		}
	}

	{
		remove_instr i("foo");
		dm.execute(i);
	}

	return 0;
}

//----------------------------------------------------------------


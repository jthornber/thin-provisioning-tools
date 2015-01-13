#include "device-mapper/ioctl_interface.h"

#include <boost/shared_ptr.hpp>
#include <iostream>
#include <list>

using namespace dm;
using namespace std;

//----------------------------------------------------------------

namespace {
	class op {
	public:
		virtual ~op() {}
		virtual void execute() = 0;
	};

	// Atomic, reversable operations
	class reversable_op : public op {
	public:
		typedef boost::shared_ptr<reversable_op> ptr;

		virtual void undo() = 0;
	};

	class compound_op : public reversable_op {
	public:
		virtual void execute() {
			list<reversable_op::ptr>::iterator it = ops_.begin(), e = ops_.end();

			try {
				while (it != e) {
					(*it)->execute();
					++it;
				}
			} catch (...) {
				while (it != ops_.begin()) {
					--it;
					(*it)->undo();
				}

				throw;
			}
		}

		virtual void undo() {
			list<reversable_op::ptr>::reverse_iterator it = ops_.rbegin(), e = ops_.rend();
			while (it != e) {
				(*it)->undo();
				--it;
			}
		}

		void prepend(reversable_op::ptr const &op) {
			ops_.push_front(op);
		}

		void append(reversable_op::ptr const &op) {
			ops_.push_back(op);
		}

	private:
		list<reversable_op::ptr> ops_;
	};

	// Some ops are reversable.  If an undo fails we do not preserve atomicity.
	//
	// reversable ops: version, list_devices, create, remove, suspend, resume, clear, load, status, table, info, wait
	// non-reversable ops: remove_all, message (depends on the message, but we can't do it generically)

	// Do we want to get intermediate results, eg. a status command run
	// in the middle of a sequence?

	class dm_sequence : public reversable_op {
	public:
		void version();

	private:
		
	};
}

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


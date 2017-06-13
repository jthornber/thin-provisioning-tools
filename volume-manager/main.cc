#include "device-mapper/ioctl_interface.h"
//#include "device-mapper/dm.h"

#include <boost/shared_ptr.hpp>
#include <iostream>
#include <list>

using namespace dm;
using namespace std;

//----------------------------------------------------------------

#if 0
namespace {

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

	uint64_t k(uint64_t n) {
		return 2 * n;
	}

	uint64_t meg(uint64_t n) {
		return 1024 * k(n);
	}

	uint64_t gig(uint64_t n) {
		return 1024 * meg(n);
	}

	//--------------------------------
	// Some vm scenarios I'm using to explore the possibilities for the
	// high level dm interface
#if 0
	void activate_linear_vol(interface &dm) {
		compiler dmc;

		device pv1 = dmc.external_volume("/dev/vdc");
		device pv2 = dmc.external_volume("/dev/vdd");

		device lv = cmd.create("lvol1", "UUIDFLKLSF");
		lv.load() << linear(gig(2), pv1, meg(900))
			  << linear(gig(1), pv2, 0);
		lv.resume();

		program activate = dmc.compile_reversible();

		// these will throw on failure
		activate();
		activate.reverse();
	}

	void thin_scenario_1(interface &dm) {
		compiler dmc;

		device fast_pv = dmc.external_volume("/dev/vdc");
		device slow_pv = dmc.external_volume("/dev/vdd");

		device md_dev = dmc.create("thin-metadata");
		md_dev.load() << linear(meg(8), fast_pv, 0);
		md_dev.resume();

		device data_dev = dmc.create("thin-data");
		data_dev.load() << linear(gig(2), slow_pv, 0);
		data_dev.resume();

		md_dev.format_thin_pool(); // irreversible

		device pool_dev = dmc.create("thin-pool");
		pool_dev.load() << pool(gig(2), metadata_dev, data_dev);
		pool_dev.resume();

		device thin_dev = dmc.create("thin1");
		pool_dev.create_thin(0)
		thin_dev.load() << thin(gig(1), pool_dev, 0);
		thin_dev.resume();

		// By giving the IGNORE_IROPS flag, we're telling the
		// compiler that we know the pool format is harmless
		// (ie. it hasn't over written any important data).
		program activate = dmc.compile_reversible(IGNORE_IROPS);

		future<thin_status> &status = thin_dev.status<thin_status>();
		program update_status = dmc.compile(); // no need for this to be reversible

		// Now that we have the pieces for our program, we can put
		// them together.
		activate(dm);

		try {
			update_status(dm);
			cout << "initial allocated blocks: " << status->allocated_blocks_ << "\n";

			// Not a program, just simulating volume use
			wipe_device(thin);

			update_status(dm);
			cout << "After wipe allocated blocks: " << status->allocated_blocks_ << "\n";
		} catch (...) {
			activate.reverse();
			throw;
		}

		activate.reverse();
	}

	//--------------------------------

	// Exactly the same as scenario 1, except we force consideration of
	// object lifetimes by returning the programs from functions.

	pair<program, program> compile_programs_for_scenario2()
	{
		compiler dmc;

		device fast_pv = dmc.external_volume("/dev/vdc");
		device slow_pv = dmc.external_volume("/dev/vdd");

		device md_dev = dmc.create("thin-metadata");
		md_dev.load() << linear(meg(8), fast_pv, 0);
		md_dev.resume();

		device data_dev = dmc.create("thin-data");
		data_dev.load() << linear(gig(2), slow_pv, 0);
		data_dev.resume();

		md_dev.format_thin_pool(); // irreversible

		device pool_dev = dmc.create("thin-pool");
		pool_dev.load() << pool(gig(2), metadata_dev, data_dev);
		pool_dev.resume();

		device thin_dev = dmc.create("thin1");
		pool_dev.create_thin(0)
		thin_dev.load() << thin(gig(1), pool_dev, 0);
		thin_dev.resume();

		// By giving the IGNORE_IROPS flag, we're telling the
		// compiler that we know the pool format is harmless
		// (ie. it hasn't over written any important data).
		// Without this flag this call would fail (throw).
		program activate = dmc.compile_reversible(IGNORE_IROPS);

		future<thin_status> &status = thin_dev.status<thin_status>();
		program update_status = dmc.compile_reversible();

		return make_pair(activate, update_status);
	}

	void thin_scenario_2(interface &dm) {
		// Program needs to be light weight so we can return it
		// by value.
		pair<program, program> p = compile_programs_for_scenario2();
		program &activate = p.first();
		program &update_status = p.second();

		activate(dm);

		// FIXME: pass a lambda to activate, so the reverse is
		// automatically done. eg, activate.ensure_reversed(...)
		try {
			update_status(dm);
			cout << "initial allocated blocks: " << status->allocated_blocks_ << "\n";

			// Not a program, just simulating volume use
			wipe_device(thin);

			update_status(dm);
			cout << "After wipe allocated blocks: " << status->allocated_blocks_ << "\n";

		} catch (...) {
			activate.reverse();
			throw;
		}

		activate.reverse();
	}
#endif
}
#endif
//----------------------------------------------------------------

int main(int argc, char **argv)
{
	ioctl_interface dm;
//	activate_linear_vol(dm);

	return 0;
}

//----------------------------------------------------------------

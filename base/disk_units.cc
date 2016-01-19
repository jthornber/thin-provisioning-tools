#include "base/disk_units.h"

#include <stdexcept>
#include <boost/lexical_cast.hpp>

using namespace std;
using namespace boost;

//----------------------------------------------------------------

unsigned long long
base::disk_unit_multiplier(disk_unit u)
{
	switch (u) {
	case UNIT_BYTE:
		return 1;

	case UNIT_SECTOR:
		return 512;

	case UNIT_kB:
		return 1000;

	case UNIT_MB:
		return 1000000;

	case UNIT_GB:
		return 1000000000ull;

	case UNIT_TB:
		return 1000000000000ull;

	case UNIT_PB:
		return 1000000000000000ull;

	case UNIT_KiB:
		return 1024ull;

	case UNIT_MiB:
		return 1024ull * 1024ull;

	case UNIT_GiB:
		return 1024ull * 1024ull * 1024ull;

	case UNIT_TiB:
		return 1024ull * 1024ull * 1024ull * 1024ull;

	case UNIT_PiB:
		return 1024ull * 1024ull * 1024ull * 1024ull * 1024ull;
	}

	throw runtime_error("unknown unit type");
	return 1;
}

string
base::format_disk_unit(unsigned long long numerator, disk_unit u)
{
	numerator *= disk_unit_multiplier(u);
	unsigned i;
	for (i = 0; numerator >= 1024; i++)
		numerator /= 1024;

	char const *extensions[] = {
		"", "KiB", "MiB", "GiB", "TiB", "PiB"
	};

	// FIXME: check subscript of i
	return lexical_cast<string>(numerator) + " " + extensions[i];
}

//----------------------------------------------------------------

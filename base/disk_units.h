#ifndef BASE_DISK_UNITS_H
#define BASE_DISK_UNITS_H

#include <string>

//----------------------------------------------------------------

namespace base {
	enum disk_unit {
		UNIT_BYTE,
		UNIT_SECTOR,

		// decimal multipliers
		UNIT_kB,
		UNIT_MB,
		UNIT_GB,
		UNIT_TB,
		UNIT_PB,

		// binary multipliers
		UNIT_KiB,
		UNIT_MiB,
		UNIT_GiB,
		UNIT_TiB,
		UNIT_PiB
	};

	unsigned long long disk_unit_multiplier(disk_unit u);
	std::string format_disk_unit(unsigned long long numerator, disk_unit u);
}

//----------------------------------------------------------------

#endif

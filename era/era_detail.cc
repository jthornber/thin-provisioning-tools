#include "era/era_detail.h"

#include <stdexcept>

using namespace base;
using namespace era;

//----------------------------------------------------------------

void
era_detail_traits::unpack(disk_type const &disk, value_type &value)
{
	value.nr_bits = to_cpu<uint32_t>(disk.nr_bits);
	value.writeset_root = to_cpu<uint64_t>(disk.writeset_root);
}

void
era_detail_traits::pack(value_type const &value, disk_type &disk)
{
	disk.nr_bits = to_disk<le32>(value.nr_bits);
	disk.writeset_root = to_disk<le64>(value.writeset_root);
}

//----------------------------------------------------------------

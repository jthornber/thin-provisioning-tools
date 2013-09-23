#include "caching/mapping_array.h"
#include "persistent-data/endian_utils.h"

using namespace caching::mapping_array_detail;

//----------------------------------------------------------------

namespace {
	const uint64_t FLAGS_MASK = (1 << 16) - 1;
}

void
mapping_traits::unpack(disk_type const &disk, value_type &value)
{
	uint64_t v = base::to_cpu<uint64_t>(disk);
	value.oblock_ = v >> 16;
	value.flags_ = v & FLAGS_MASK;
}

void
mapping_traits::pack(value_type const &value, disk_type &disk)
{
	uint64_t packed = value.oblock_ << 16;
	packed = packed | (value.flags_ & FLAGS_MASK);
	disk = base::to_disk<le64>(packed);
}

//----------------------------------------------------------------

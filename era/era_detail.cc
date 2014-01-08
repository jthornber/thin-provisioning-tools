#include "era/era_detail.h"

#include <stdexcept>

using namespace base;
using namespace era;

//----------------------------------------------------------------

namespace {
#if 0
	le32 pack_hash_detail(uint32_t hash1, uint32_t hash2, uint32_t nr_probes) {
		throw std::runtime_error("not implemented");
	}

	void unpack_hash_detail(le32 packed, uint32_t &hash1, uint32_t &hash2, uint32_t &nr_probes) {
		throw std::runtime_error("not implemented");
	}
#endif
}

void
era_detail_traits::unpack(disk_type const &disk, value_type &value)
{
	value.nr_blocks = to_cpu<uint32_t>(disk.nr_blocks);
	value.nr_bits = to_cpu<uint32_t>(disk.nr_bits);
	value.nr_set = to_cpu<uint32_t>(disk.nr_set);
	value.bloom_root = to_cpu<uint64_t>(disk.bloom_root);

	//unpack_hash_detail(disk.hash_fns_and_probes, value.hash1, value.hash2, value.nr_probes);
}

void
era_detail_traits::pack(value_type const &value, disk_type &disk)
{
	disk.nr_blocks = to_disk<le32>(value.nr_blocks);
	disk.nr_bits = to_disk<le32>(value.nr_bits);
	disk.nr_set = to_disk<le32>(value.nr_set);
	disk.bloom_root = to_disk<le64>(value.bloom_root);
//	disk.hash_fns_and_probes = pack_hash_detail(value.hash1, value.hash2, value.nr_probes);
}

//----------------------------------------------------------------

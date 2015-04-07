#ifndef PERSISTENT_DATA_DATA_STRUCTURES_SIMPLE_TRAITS_H
#define PERSISTENT_DATA_DATA_STRUCTURES_SIMPLE_TRAITS_H

//----------------------------------------------------------------

namespace persistent_data {
	struct uint64_traits {
		typedef base::le64 disk_type;
		typedef uint64_t value_type;
		typedef no_op_ref_counter<uint64_t> ref_counter;

		static void unpack(disk_type const &disk, value_type &value) {
			value = base::to_cpu<uint64_t>(disk);
		}

		static void pack(value_type const &value, disk_type &disk) {
			disk = base::to_disk<base::le64>(value);
		}
	};

	struct uint32_traits {
		typedef base::le32 disk_type;
		typedef uint32_t value_type;
		typedef no_op_ref_counter<uint32_t> ref_counter;

		static void unpack(disk_type const &disk, value_type &value) {
			value = base::to_cpu<uint32_t>(disk);
		}

		static void pack(value_type const &value, disk_type &disk) {
			disk = base::to_disk<base::le32>(value);
		}
	};
}

//----------------------------------------------------------------

#endif

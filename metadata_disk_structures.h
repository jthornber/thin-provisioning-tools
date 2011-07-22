#ifndef METADATA_DISK_STRUCTURES_H
#define METADATA_DISK_STRUCTURES_H

#include "endian.h"
#include "btree.h"

//----------------------------------------------------------------

namespace thin_provisioning {
	using namespace base;	// FIXME: don't use namespaces in headers.

	struct device_details_disk {
		__le64 dev_size_;
		__le64 mapped_blocks_;
		__le64 transaction_id_;  /* when created */
		__le32 creation_time_;
		__le32 snapshotted_time_;
	} __attribute__ ((packed));

	struct device_details {
		uint64_t dev_size_;
		uint64_t mapped_blocks_;
		uint64_t transaction_id_;  /* when created */
		uint32_t creation_time_;
		uint32_t snapshotted_time_;
	};

	struct device_details_traits {
		typedef device_details_disk disk_type;
		typedef device_details value_type;
		typedef persistent_data::NoOpRefCounter<device_details> ref_counter;

		static void unpack(device_details_disk const &disk, device_details &value);
		static void pack(device_details const &value, device_details_disk &disk);
	};

	unsigned const SPACE_MAP_ROOT_SIZE = 128;

	typedef unsigned char __u8;

	struct superblock_disk {
		__le32 csum_;
		__le32 flags_;
		__le64 blocknr_;

		__u8 uuid_[16];
		__le64 magic_;
		__le32 version_;
		__le32 time_;

		__le64 trans_id_;
		/* root for userspace's transaction (for migration and friends) */
		__le64 held_root_;

		__u8 data_space_map_root_[SPACE_MAP_ROOT_SIZE];
		__u8 metadata_space_map_root_[SPACE_MAP_ROOT_SIZE];

		/* 2 level btree mapping (dev_id, (dev block, time)) -> data block */
		__le64 data_mapping_root_;

		/* device detail root mapping dev_id -> device_details */
		__le64 device_details_root_;

		__le32 data_block_size_; /* in 512-byte sectors */

		__le32 metadata_block_size_; /* in 512-byte sectors */
		__le64 metadata_nr_blocks_;

		__le32 compat_flags_;
		__le32 incompat_flags_;
	} __attribute__ ((packed));

	struct superblock {
		uint32_t csum_;
		uint32_t flags_;
		uint64_t blocknr_;

		unsigned char uuid_[16];
		uint64_t magic_;
		uint32_t version_;
		uint32_t time_;

		uint64_t trans_id_;
		/* root for userspace's transaction (for migration and friends) */
		uint64_t held_root_;

		unsigned char data_space_map_root_[SPACE_MAP_ROOT_SIZE];
		unsigned char metadata_space_map_root_[SPACE_MAP_ROOT_SIZE];

		/* 2 level btree mapping (dev_id, (dev block, time)) -> data block */
		uint64_t data_mapping_root_;

		/* device detail root mapping dev_id -> device_details */
		uint64_t device_details_root_;

		uint32_t data_block_size_; /* in 512-byte sectors */

		uint32_t metadata_block_size_; /* in 512-byte sectors */
		uint64_t metadata_nr_blocks_;

		uint32_t compat_flags_;
		uint32_t incompat_flags_;
	};

	struct superblock_traits {
		typedef superblock_disk disk_type;
		typedef superblock value_type;
		typedef NoOpRefCounter<superblock> ref_counter;

		static void unpack(superblock_disk const &disk, superblock &value);
		static void pack(superblock const &value, superblock_disk &disk);
	};
}

//----------------------------------------------------------------

#endif

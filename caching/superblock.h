#ifndef CACHE_SUPERBLOCK_H
#define CACHE_SUPERBLOCK_H

#include "base/endian_utils.h"
#include "persistent-data/data-structures/btree.h"

#include <set>

//----------------------------------------------------------------

namespace caching {
	typedef unsigned char __u8;

	unsigned const SPACE_MAP_ROOT_SIZE = 128;
	unsigned const CACHE_POLICY_NAME_SIZE = 16;
	unsigned const CACHE_POLICY_VERSION_SIZE = 3;
	block_address const SUPERBLOCK_LOCATION = 0;

	class superblock_flags {
	public:
		enum flag {
			CLEAN_SHUTDOWN
		};

		enum flag_bits {
			CLEAN_SHUTDOWN_BIT = 0
		};

		superblock_flags();
		superblock_flags(uint32_t bits);

		void set_flag(flag f);
		void clear_flag(flag f);
		bool get_flag(flag f) const;
		uint32_t encode() const;
		uint32_t get_unhandled_flags() const;

	private:
		uint32_t unhandled_flags_;
		std::set<flag> flags_;
	};

	struct superblock {
		superblock();

		uint32_t csum;
		superblock_flags flags;
		uint64_t blocknr;

		__u8 uuid[16];
		uint64_t magic;
		uint32_t version;

		__u8 policy_name[CACHE_POLICY_NAME_SIZE];
		uint32_t policy_version[CACHE_POLICY_VERSION_SIZE];
		uint32_t policy_hint_size;

		__u8 metadata_space_map_root[SPACE_MAP_ROOT_SIZE];

		uint64_t mapping_root;
		uint64_t hint_root;

		uint64_t discard_root;
		uint64_t discard_block_size;
		uint64_t discard_nr_blocks;

		uint32_t data_block_size; /* in 512-byte sectors */
		uint32_t metadata_block_size; /* in 512-byte sectors */
		uint32_t cache_blocks;

		uint32_t compat_flags;
		uint32_t compat_ro_flags;
		uint32_t incompat_flags;

		uint32_t read_hits;
		uint32_t read_misses;
		uint32_t write_hits;
		uint32_t write_misses;
	};

	enum incompat_bits {
		VARIABLE_HINT_SIZE_BIT = 0
	};

	//--------------------------------

	namespace superblock_damage {

		class damage_visitor;

		class damage {
		public:
			damage(std::string const &desc)
				: desc_(desc) {
			}

			virtual ~damage() {}
			virtual void visit(damage_visitor &v) const = 0;

			std::string const &get_desc() const {
				return desc_;
			}

		private:
			std::string desc_;
		};

		struct superblock_corrupt : public damage {
			superblock_corrupt(std::string const &desc);
			void visit(damage_visitor &v) const;
		};

		struct superblock_invalid : public damage {
			superblock_invalid(std::string const &desc);
			void visit(damage_visitor &v) const;
		};

		class damage_visitor {
		public:
			virtual ~damage_visitor() {}

			void visit(damage const &d);

			virtual void visit(superblock_corrupt const &d) = 0;
			virtual void visit(superblock_invalid const &d) = 0;
		};
	}

	//--------------------------------

	bcache::validator::ptr superblock_validator();

	superblock read_superblock(persistent_data::block_manager<>::ptr bm,
				   persistent_data::block_address location = SUPERBLOCK_LOCATION);

	void write_superblock(persistent_data::block_manager<>::ptr bm,
			      superblock const &sb,
			      persistent_data::block_address location = SUPERBLOCK_LOCATION);

	void check_superblock(superblock const &sb,
			      persistent_data::block_address nr_metadata_blocks,
			      superblock_damage::damage_visitor &visitor);

	void check_superblock(persistent_data::block_manager<>::ptr bm,
			      persistent_data::block_address nr_metadata_blocks,
			      superblock_damage::damage_visitor &visitor);
}

//----------------------------------------------------------------

#endif

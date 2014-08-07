#ifndef ERA_SUPERBLOCK_H
#define ERA_SUPERBLOCK_H

#include "persistent-data/block.h"
#include "era/era_detail.h"
#include <boost/optional.hpp>

#include <set>

//----------------------------------------------------------------

namespace era {
	typedef unsigned char __u8;

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

	unsigned const SPACE_MAP_ROOT_SIZE = 128;
	uint64_t const SUPERBLOCK_LOCATION = 0;

	struct superblock {
		superblock();

		uint32_t csum;
		uint64_t blocknr;
		superblock_flags flags;

		__u8 uuid[16];	// FIXME: do we really need this?
		uint64_t magic;
		uint32_t version;

		__u8 metadata_space_map_root[SPACE_MAP_ROOT_SIZE];

		uint32_t data_block_size;
		uint32_t metadata_block_size;
		uint32_t nr_blocks;

		uint32_t current_era;
		era_detail current_detail;

		// A btree of undigested era_details
		uint64_t writeset_tree_root;

		// Big array holding the digested era/block info.
		uint64_t era_array_root;

		boost::optional<persistent_data::block_address> metadata_snap;
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

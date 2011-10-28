#ifndef MULTISNAP_METADATA_H
#define MULTISNAP_METADATA_H

#include "metadata_ll.h"

#include <string>
#include <boost/shared_ptr.hpp>

//----------------------------------------------------------------

namespace thin_provisioning {
	class metadata;
	class thin {
	public:
		typedef boost::shared_ptr<thin> ptr;
		typedef boost::optional<block_time> maybe_address;

		thin_dev_t get_dev_t() const;
		maybe_address lookup(block_address thin_block);
		void insert(block_address thin_block, block_address data_block);
		void remove(block_address thin_block);

		void set_snapshot_time(uint32_t time);

		block_address get_mapped_blocks() const;
		void set_mapped_blocks(block_address count);

	private:
		friend class metadata;
		thin(thin_dev_t dev, metadata *metadata);

		thin_dev_t dev_;
		metadata *metadata_;
	};

	class metadata {
	public:
		typedef boost::shared_ptr<metadata> ptr;

		metadata(metadata_ll::ptr md);
		~metadata();

		void create_thin(thin_dev_t dev);
		void create_snap(thin_dev_t dev, thin_dev_t origin);
		void del(thin_dev_t);

		void set_transaction_id(uint64_t id);
		uint64_t get_transaction_id() const;

		block_address get_held_root() const;

		block_address alloc_data_block();
		void free_data_block(block_address b);

		// accessors
		block_address get_nr_free_data_blocks() const;
		sector_t get_data_block_size() const;
		block_address get_data_dev_size() const;

		thin::ptr open_thin(thin_dev_t);

	private:
		friend class thin;

		bool device_exists(thin_dev_t dev) const;

		metadata_ll::ptr md_;
	};
};

//----------------------------------------------------------------

#endif

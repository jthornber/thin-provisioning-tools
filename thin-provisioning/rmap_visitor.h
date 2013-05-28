#ifndef THIN_RMAP_VISITOR_H
#define THIN_RMAP_VISITOR_H

#include "persistent-data/run.h"
#include "thin-provisioning/mapping_tree.h"

//----------------------------------------------------------------

namespace thin_provisioning {
	// This value visitor (see btree_damage_visitor.h) produces a
	// reverse mapping for parts of the data device.
	//
	// i) Use add_data_region() to specify which areas of the data device you're interested in.
	// ii) visit the mapping tree
	// iii) call complete()
	// iv) get the rmaps with get_rmap();
	class rmap_visitor {
	public:
		typedef run<block_address> region;

		rmap_visitor();

		// Specify which regions of the data device you want the rmap for.
		void add_data_region(region const &r);
		void visit(btree_path const &path, mapping_tree_detail::block_time const &bt);

		struct rmap_region {
			// FIXME: surely we don't need to provide this for
			// a POD structure?
			bool operator ==(rmap_region const &rhs) const {
				return ((data_begin == rhs.data_begin) &&
					(data_end == rhs.data_end) &&
					(thin_dev == rhs.thin_dev) &&
					(thin_begin == rhs.thin_begin));
			}

			block_address data_begin;
		        block_address data_end;

			uint32_t thin_dev;
			block_address thin_begin;
		};

		void complete();
		vector<rmap_region> const &get_rmap() const;

	private:
		bool in_regions(block_address b) const;
		bool adjacent_block(rmap_region const &rr,
				    uint32_t thin_dev, block_address thin_block,
				    block_address data_block) const;
		void insert_new_region(uint32_t thin_dev, block_address thin_block,
				       block_address data_block);
		void push_current();

		void visit_block(uint32_t thin_dev, block_address thin_block,
				 block_address data_block);

		vector<region> regions_;

		boost::optional<rmap_region> current_rmap_;
		vector<rmap_region> rmap_;
	};
}

//----------------------------------------------------------------

#endif

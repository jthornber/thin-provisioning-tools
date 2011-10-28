#include "metadata.h"

using namespace persistent_data;
using namespace thin_provisioning;

//----------------------------------------------------------------

namespace {
	class mappings_extractor : public btree<2, block_traits>::visitor {
	public:
		typedef boost::shared_ptr<mappings_extractor> ptr;

		mappings_extractor(uint64_t dev_id, emitter::ptr e,
				   space_map::ptr md_sm, space_map::ptr data_sm)
			: dev_id_(dev_id),
			  e_(e),
			  md_sm_(md_sm),
			  data_sm_(data_sm),
			  in_range_(false) {
		}

		bool visit_internal(unsigned level, bool sub_root, boost::optional<uint64_t> key,
				    btree_detail::node_ref<uint64_traits> const &n) {
			return (sub_root && key) ? (*key == dev_id_) : true;
		}

		bool visit_internal_leaf(unsigned level, bool sub_root, boost::optional<uint64_t> key,
					 btree_detail::node_ref<uint64_traits> const &n) {
			return true;
		}

		bool visit_leaf(unsigned level, bool sub_root, boost::optional<uint64_t> maybe_key,
				btree_detail::node_ref<block_traits> const &n) {
			for (unsigned i = 0; i < n.get_nr_entries(); i++) {
				add_mapping(n.key_at(i), n.value_at(i).block_);
			}

			return true;
		}


		void visit_complete() {
			end_mapping();
		}

	private:
		void start_mapping(uint64_t origin_block, uint64_t dest_block) {
			origin_start_ = origin_block;
			dest_start_ = dest_block;
			len_ = 1;
			in_range_ = true;
		}

		void end_mapping() {
			if (in_range_) {
				if (len_ == 1)
					e_->single_map(origin_start_, dest_start_);
				else
					e_->range_map(origin_start_, dest_start_, len_);

				in_range_ = false;
			}
		}

		void add_mapping(uint64_t origin_block, uint64_t dest_block) {
			if (!in_range_)
				start_mapping(origin_block, dest_block);

			else if (origin_block == origin_start_ + len_ &&
				 dest_block == dest_start_ + len_)
				len_++;

			else {
				end_mapping();
				start_mapping(origin_block, dest_block);
			}
		}

		uint64_t dev_id_;
		emitter::ptr e_;
		space_map::ptr md_sm_;
		space_map::ptr data_sm_;

		bool in_range_;
		uint64_t origin_start_, dest_start_, len_;

	};

	class details_extractor : public btree<1, device_details_traits>::visitor {
	public:
		typedef boost::shared_ptr<details_extractor> ptr;

		details_extractor() {
		}

		bool visit_internal(unsigned level, bool sub_root, boost::optional<uint64_t> key,
				    btree_detail::node_ref<uint64_traits> const &n) {
			return true;
		}

		bool visit_internal_leaf(unsigned level, bool sub_root, boost::optional<uint64_t> key,
					 btree_detail::node_ref<uint64_traits> const &n) {
			return true;
		}

		bool visit_leaf(unsigned level, bool sub_root, boost::optional<uint64_t> maybe_key,
				btree_detail::node_ref<device_details_traits> const &n) {
			for (unsigned i = 0; i < n.get_nr_entries(); i++)
				devices_.insert(make_pair(n.key_at(i), n.value_at(i)));

			return true;
		}

		map<uint64_t, device_details> const &get_devices() const {
			return devices_;
		}

	private:
		map<uint64_t, device_details> devices_;
	};
}

void
metadata::dump(emitter::ptr e)
{
	e->begin_superblock("", md_->sb_.time_, md_->sb_.trans_id_, md_->sb_.data_block_size_);

	details_extractor::ptr de(new details_extractor);

	md_->details_.visit(de);
	map<uint64_t, device_details> const &devs = de->get_devices();

	map<uint64_t, device_details>::const_iterator it, end = devs.end();
	for (it = devs.begin(); it != end; ++it) {
		uint64_t dev_id = it->first;
		device_details const &dd = it->second;

		e->begin_device(dev_id,
				dd.mapped_blocks_,
				dd.transaction_id_,
				dd.creation_time_,
				dd.snapshotted_time_);

		mappings_extractor::ptr me(new mappings_extractor(dev_id, e, md_->metadata_sm_, md_->data_sm_));
		md_->mappings_.visit(me);

		e->end_device();
	}

	e->end_superblock();
}

//----------------------------------------------------------------

// Copyright (C) 2011 Red Hat, Inc. All rights reserved.
//
// This file is part of the thin-provisioning-tools source.
//
// thin-provisioning-tools is free software: you can redistribute it
// and/or modify it under the terms of the GNU General Public License
// as published by the Free Software Foundation, either version 3 of
// the License, or (at your option) any later version.
//
// thin-provisioning-tools is distributed in the hope that it will be
// useful, but WITHOUT ANY WARRANTY; without even the implied warranty
// of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License along
// with thin-provisioning-tools.  If not, see
// <http://www.gnu.org/licenses/>.

#include "thin-provisioning/override_emitter.h"
#include "thin-provisioning/restore_emitter.h"
#include "thin-provisioning/superblock.h"

using namespace std;
using namespace thin_provisioning;

//----------------------------------------------------------------

namespace {
	using namespace superblock_detail;

	class restorer : public emitter {
	public:
		restorer(metadata::ptr md)
			: md_(md),
			  in_superblock_(false),
			  nr_data_blocks_(),
			  empty_mapping_(new_mapping_tree()) {
		}

		virtual ~restorer() {
			// FIXME: replace with a call to empty_mapping_->destroy()
			md_->metadata_sm_->dec(empty_mapping_->get_root());
		}

		virtual void begin_superblock(std::string const &uuid,
					      uint64_t time,
					      uint64_t trans_id,
					      boost::optional<uint32_t> flags,
					      boost::optional<uint32_t> version,
					      uint32_t data_block_size,
					      uint64_t nr_data_blocks,
					      boost::optional<uint64_t> metadata_snap) {
			in_superblock_ = true;
			nr_data_blocks_ = nr_data_blocks;
			superblock &sb = md_->sb_;
			memset(&sb.uuid_, 0, sizeof(sb.uuid_));
			memcpy(&sb.uuid_, uuid.c_str(), std::min(sizeof(sb.uuid_), uuid.length()));
			sb.time_ = time;
			sb.trans_id_ = trans_id;
			sb.flags_ = flags ? *flags : 0;
			sb.version_ = version ? *version : 1;
			sb.data_block_size_ = data_block_size;
			sb.metadata_snap_ = metadata_snap ? *metadata_snap : 0;
			md_->data_sm_->extend(nr_data_blocks);
		}

		virtual void end_superblock() {
			if (!in_superblock_)
				throw runtime_error("missing superblock");

			md_->commit();
			in_superblock_ = false;
		}

		virtual void begin_device(uint32_t dev,
					  uint64_t mapped_blocks,
					  uint64_t trans_id,
					  uint64_t creation_time,
					  uint64_t snap_time) {
			if (!in_superblock_)
				throw runtime_error("missing superblock");

			if (device_exists(dev))
				throw std::runtime_error("Device already exists");

			// Store the entry of the details tree
			current_device_details_.mapped_blocks_ = 0;
			current_device_details_.transaction_id_ = trans_id;
			current_device_details_.creation_time_ = (uint32_t)creation_time;
			current_device_details_.snapshotted_time_ = (uint32_t)snap_time;

			current_mapping_ = empty_mapping_->clone();
			current_device_ = boost::optional<uint32_t>(dev);
		}

		virtual void end_device() {
			uint64_t key[1] = {*current_device_};

			// Add entry to the details tree
			md_->details_->insert(key, current_device_details_);

			md_->mappings_top_level_->insert(key, current_mapping_->get_root());
			md_->mappings_->set_root(md_->mappings_top_level_->get_root()); // FIXME: ugly

			current_device_ = boost::optional<uint32_t>();
		}

		virtual void begin_named_mapping(std::string const &name) {
			throw runtime_error("not implemented");
		}

		virtual void end_named_mapping() {
			throw runtime_error("not implemented");
		}

		virtual void identifier(std::string const &name) {
			throw runtime_error("not implemented");
		}

		virtual void range_map(uint64_t origin_begin, uint64_t data_begin, uint32_t time, uint64_t len) {
			for (uint64_t i = 0; i < len; i++)
				single_map(origin_begin++, data_begin++, time);
		}

		virtual void single_map(uint64_t origin_block, uint64_t data_block, uint32_t time) {
			if (!current_device_)
				throw runtime_error("not in device");

			if (data_block >= nr_data_blocks_) {
				std::ostringstream out;
				out << "mapping beyond end of data device (" << data_block
				    << " >= " << nr_data_blocks_ << ")";
				throw std::runtime_error(out.str());
			}

			uint64_t key[1] = {origin_block};
			mapping_tree_detail::block_time bt;
			bt.block_ = data_block;
			bt.time_ = time;
			current_device_details_.mapped_blocks_ +=
				static_cast<uint64_t>(current_mapping_->insert(key, bt));
			md_->data_sm_->inc(data_block);
		}

	private:
		single_mapping_tree::ptr new_mapping_tree() {
			return single_mapping_tree::ptr(
				new single_mapping_tree(*md_->tm_,
							mapping_tree_detail::block_time_ref_counter(md_->data_sm_)));
		}

		bool device_exists(thin_dev_t dev) const {
			uint64_t key[1] = {dev};
			device_tree::maybe_value v = md_->details_->lookup(key);
			return !!v;
		}

		metadata::ptr md_;
		override_options opts_;

		bool in_superblock_;
		block_address nr_data_blocks_;
		boost::optional<uint32_t> current_device_;
		device_tree_detail::device_details current_device_details_;
		single_mapping_tree::ptr current_mapping_;
		single_mapping_tree::ptr empty_mapping_;
	};
}

//----------------------------------------------------------------

emitter::ptr
thin_provisioning::create_restore_emitter(metadata::ptr md)
{
	return emitter::ptr(new restorer(md));
}

//----------------------------------------------------------------

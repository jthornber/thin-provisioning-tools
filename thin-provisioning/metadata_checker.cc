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

#include "thin-provisioning/file_utils.h"
#include "thin-provisioning/metadata.h"
#include "thin-provisioning/metadata_checker.h"

using namespace persistent_data;
using namespace thin_provisioning;

//----------------------------------------------------------------
#if 0
void
metadata_damage::set_message(std::string const &message)
{
	message_ = message;
}

std::string const &
metadata_damage::get_message() const
{
	return message_;
}

//--------------------------------

void
super_block_corruption::visit(metadata_damage_visitor &visitor) const
{
	visitor.visit(*this);
}

bool
super_block_corruption::operator ==(super_block_corruption const &rhs) const
{
	return true;
}

//--------------------------------

missing_device_details::missing_device_details(range64 missing)
	: missing_(missing)
{
}

void
missing_device_details::visit(metadata_damage_visitor &visitor) const
{
	visitor.visit(*this);
}

bool
missing_device_details::operator ==(missing_device_details const &rhs) const
{
	return missing_ == rhs.missing_;
}

//--------------------------------

missing_devices::missing_devices(range64 missing)
	: missing_(missing)
{
}

void
missing_devices::visit(metadata_damage_visitor &visitor) const
{
	visitor.visit(*this);
}

bool
missing_devices::operator ==(missing_devices const &rhs) const
{
	return missing_ == rhs.missing_;
}

//--------------------------------

missing_mappings::missing_mappings(uint64_t dev, range64 missing)
	: dev_(dev),
	  missing_(missing)
{
}

void
missing_mappings::visit(metadata_damage_visitor &visitor) const
{
	visitor.visit(*this);
}

bool
missing_mappings::operator ==(missing_mappings const &rhs) const
{
	return dev_ == rhs.dev_ && missing_ == rhs.missing_;
}

//--------------------------------

bad_metadata_ref_count::bad_metadata_ref_count(block_address b,
					       ref_t actual,
					       ref_t expected)
	: b_(b),
	  actual_(actual),
	  expected_(expected)
{
}

void
bad_metadata_ref_count::visit(metadata_damage_visitor &visitor) const
{
	visitor.visit(*this);
}

bool
bad_metadata_ref_count::operator ==(bad_metadata_ref_count const &rhs) const
{
	return b_ == rhs.b_ && actual_ == rhs.actual_ && expected_ == rhs.expected_;
}

//--------------------------------

bad_data_ref_count::bad_data_ref_count(block_address b,
				       ref_t actual,
				       ref_t expected)
	: b_(b),
	  actual_(actual),
	  expected_(expected)
{
}

void
bad_data_ref_count::visit(metadata_damage_visitor &visitor) const
{
	visitor.visit(*this);
}

bool
bad_data_ref_count::operator ==(bad_data_ref_count const &rhs) const
{
	return b_ == rhs.b_ && actual_ == rhs.actual_ && expected_ == rhs.expected_;
}

//--------------------------------

missing_metadata_ref_counts::missing_metadata_ref_counts(range64 missing)
	: missing_(missing)
{
}

void
missing_metadata_ref_counts::visit(metadata_damage_visitor &visitor) const
{
	visitor.visit(*this);
}

bool
missing_metadata_ref_counts::operator ==(missing_metadata_ref_counts const &rhs) const
{
	return missing_ == rhs.missing_;
}

//--------------------------------

missing_data_ref_counts::missing_data_ref_counts(range64 missing)
	: missing_(missing)
{
}

void
missing_data_ref_counts::visit(metadata_damage_visitor &visitor) const
{
	visitor.visit(*this);
}

bool
missing_data_ref_counts::operator ==(missing_data_ref_counts const &rhs) const
{
	return missing_ == rhs.missing_;
}

//--------------------------------

void
metadata_damage_visitor::visit(metadata_damage const &damage)
{
	damage.visit(*this);
}

//--------------------------------

checker::checker(block_manager::ptr bm)
	: bm_(bm)
{
}

//----------------------------------------------------------------

#if 0


namespace {
	// As well as the standard btree checks, we build up a set of what
	// devices having mappings defined, which can later be cross
	// referenced with the details tree.  A separate block_counter is
	// used to later verify the data space map.
	class mapping_validator : public btree<2, block_traits>::visitor {
	public:
		typedef boost::shared_ptr<mapping_validator> ptr;
		typedef btree_checker<2, block_traits> checker;

		mapping_validator(block_counter &metadata_counter, block_counter &data_counter)
			: checker_(metadata_counter),
			  data_counter_(data_counter)
		{
		}

		bool visit_internal(unsigned level,
				    bool sub_root,
				    optional<uint64_t> key,
				    btree_detail::node_ref<uint64_traits> const &n) {
			return checker_.visit_internal(level, sub_root, key, n);
		}

		bool visit_internal_leaf(unsigned level,
					 bool sub_root,
					 optional<uint64_t> key,
					 btree_detail::node_ref<uint64_traits> const &n) {

			bool r = checker_.visit_internal_leaf(level, sub_root, key, n);

			for (unsigned i = 0; i < n.get_nr_entries(); i++)
				devices_.insert(n.key_at(i));

			return r;
		}

		bool visit_leaf(unsigned level,
				bool sub_root,
				optional<uint64_t> key,
				btree_detail::node_ref<block_traits> const &n) {
			bool r = checker_.visit_leaf(level, sub_root, key, n);

			if (r)
				for (unsigned i = 0; i < n.get_nr_entries(); i++)
					data_counter_.inc(n.value_at(i).block_);

			return r;
		}

		set<uint64_t> const &get_devices() const {
			return devices_;
		}

	private:
	        checker checker_;
		block_counter &data_counter_;
		set<uint64_t> devices_;
	};

	struct check_count : public space_map::iterator {
		check_count(string const &desc, block_counter const &expected)
			: bad_(false),
			  expected_(expected),
			  errors_(new error_set(desc)) {
		}

		virtual void operator() (block_address b, ref_t actual) {
			ref_t expected = expected_.get_count(b);

			if (actual != expected) {
				ostringstream out;
				out << b << ": was " << actual
				    << ", expected " << expected;
				errors_->add_child(out.str());
				bad_ = true;
			}
		}

		bool bad_;
		block_counter const &expected_;
		error_set::ptr errors_;
	};

	optional<error_set::ptr>
	check_ref_counts(string const &desc, block_counter const &counts,
			 space_map::ptr sm) {

		check_count checker(desc, counts);
		sm->iterate(checker);
		return checker.bad_ ? optional<error_set::ptr>(checker.errors_) : optional<error_set::ptr>();
	}

	class metadata_checker {
	public:
		metadata_checker(string const &dev_path)
		: bm_(open_bm(dev_path)),
		  errors_(new error_set("Errors in metadata")) {
		}

		boost::optional<error_set::ptr> check() {
#if 1
			superblock sb = read_superblock();

			// FIXME: check version?

			check_mappings();

			return (errors_->get_children().size() > 0) ?
				optional<error_set::ptr>(errors_) :
				optional<error_set::ptr>();
#else
			error_set::ptr errors(new error_set("Errors in metadata"));


			if (md->sb_.metadata_snap_) {
				block_manager<>::ptr bm = md->tm_->get_bm();


				block_address root = md->sb_.metadata_snap_;

				metadata_counter.inc(root);

				superblock sb;
				block_manager<>::read_ref r = bm->read_lock(root);
				superblock_disk const *sbd = reinterpret_cast<superblock_disk const *>(&r.data());
				superblock_traits::unpack(*sbd, sb);

				metadata_counter.inc(sb.data_mapping_root_);
				metadata_counter.inc(sb.device_details_root_);
			}


			set<uint64_t> const &mapped_devs = mv->get_devices();
			details_validator::ptr dv(new details_validator(metadata_counter));
			md->details_->visit(dv);

			set<uint64_t> const &details_devs = dv->get_devices();

			for (set<uint64_t>::const_iterator it = mapped_devs.begin(); it != mapped_devs.end(); ++it)
				if (details_devs.count(*it) == 0) {
					ostringstream out;
					out << "mapping exists for device " << *it
					    << ", yet there is no entry in the details tree.";
					throw runtime_error(out.str());
				}

			metadata_counter.inc(SUPERBLOCK_LOCATION);
			md->metadata_sm_->check(metadata_counter);

			md->data_sm_->check(metadata_counter);
			errors->add_child(check_ref_counts("Errors in metadata block reference counts",
							   metadata_counter, md->metadata_sm_));
			errors->add_child(check_ref_counts("Errors in data block reference counts",
							   data_counter, md->data_sm_));

			return (errors->get_children().size() > 0) ?
				optional<error_set::ptr>(errors) :
				optional<error_set::ptr>();
#endif
		}

	private:
		static block_manager<>::ptr
		open_bm(string const &dev_path) {
			block_address nr_blocks = thin_provisioning::get_nr_blocks(dev_path);
			return block_manager<>::ptr(new block_manager<>(dev_path, nr_blocks, 1, block_io<>::READ_ONLY));
		}

		// FIXME: common code with metadata.cc
		superblock read_superblock() {
			superblock sb;
			block_manager<>::read_ref r = bm_->read_lock(SUPERBLOCK_LOCATION, superblock_validator());
			superblock_disk const *sbd = reinterpret_cast<superblock_disk const *>(&r.data());
			superblock_traits::unpack(*sbd, sb);
			return sb;
		}

		void check_mappings() {
			mapping_validator::ptr mv(
				new mapping_validator(metadata_counter_,
						      data_counter_));

			

			md->mappings_->visit(mv);
		}

		typedef block_manager<>::read_ref read_ref;
		typedef block_manager<>::write_ref write_ref;
		typedef boost::shared_ptr<metadata> ptr;

		block_manager<>::ptr bm_;
		error_set::ptr errors_;

		block_counter metadata_counter_, data_counter_;


#if 0
		tm_ptr tm_;
		superblock sb_;

		checked_space_map::ptr metadata_sm_;
		checked_space_map::ptr data_sm_;
		detail_tree::ptr details_;
		dev_tree::ptr mappings_top_level_;
		mapping_tree::ptr mappings_;
#endif
	};
}


//----------------------------------------------------------------

boost::optional<error_set::ptr>
thin_provisioning::metadata_check(std::string const &dev_path)
{
	metadata_checker checker(dev_path);
	return checker.check();
}

//----------------------------------------------------------------
#endif
#endif

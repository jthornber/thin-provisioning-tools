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

#include "base/nested_output.h"
#include "persistent-data/file_utils.h"
#include "thin-provisioning/metadata.h"
#include "thin-provisioning/metadata_checker.h"
#include "thin-provisioning/metadata_counter.h"
#include "thin-provisioning/superblock.h"

using namespace persistent_data;
using namespace thin_provisioning;

//----------------------------------------------------------------

namespace {
	class superblock_reporter : public superblock_detail::damage_visitor {
	public:
		superblock_reporter(nested_output &out)
		: out_(out),
		  err_(NO_ERROR) {
		}

		virtual void visit(superblock_detail::superblock_corruption const &d) {
			out_ << "superblock is corrupt" << end_message();
			{
				nested_output::nest _ = out_.push();
				out_ << d.desc_ << end_message();
			}
			err_ << FATAL;
		}

		base::error_state get_error() const {
			return err_;
		}

	private:
		nested_output &out_;
		error_state err_;
	};

	//--------------------------------

	class devices_reporter : public device_tree_detail::damage_visitor {
	public:
		devices_reporter(nested_output &out)
		: out_(out),
		  err_(NO_ERROR) {
		}

		virtual void visit(device_tree_detail::missing_devices const &d) {
			out_ << "missing devices: " << d.keys_ << end_message();
			{
				nested_output::nest _ = out_.push();
				out_ << d.desc_ << end_message();
			}

			err_ << FATAL;
		}

		error_state get_error() const {
			return err_;
		}

	private:
		nested_output &out_;
		error_state err_;
	};

	//--------------------------------

	class mapping_reporter : public mapping_tree_detail::damage_visitor {
	public:
		mapping_reporter(nested_output &out)
		: out_(out),
		  err_(NO_ERROR) {
		}

		virtual void visit(mapping_tree_detail::missing_devices const &d) {
			out_ << "missing all mappings for devices: " << d.keys_ << end_message();
			{
				nested_output::nest _ = out_.push();
				out_ << d.desc_ << end_message();
			}
			err_ << FATAL;
		}

		virtual void visit(mapping_tree_detail::missing_mappings const &d) {
			out_ << "thin device " << d.thin_dev_ << " is missing mappings " << d.keys_ << end_message();
			{
				nested_output::nest _ = out_.push();
				out_ << d.desc_ << end_message();
			}
			err_ << FATAL;
		}

		error_state get_error() const {
			return err_;
		}

	private:
		nested_output &out_;
		error_state err_;
	};

	//--------------------------------

	error_state examine_superblock(block_manager<>::ptr bm,
				       nested_output &out) {
		out << "examining superblock" << end_message();
		nested_output::nest _ = out.push();

		superblock_reporter sb_rep(out);
		check_superblock(bm, sb_rep);

		return sb_rep.get_error();
	}

	error_state examine_devices_tree_(transaction_manager::ptr tm,
					  superblock_detail::superblock const &sb,
					  nested_output &out) {
		out << "examining devices tree" << end_message();
		nested_output::nest _ = out.push();

		devices_reporter dev_rep(out);
		device_tree dtree(*tm, sb.device_details_root_,
				  device_tree_detail::device_details_traits::ref_counter());
		check_device_tree(dtree, dev_rep);

		return dev_rep.get_error();
	}

	error_state examine_top_level_mapping_tree_(transaction_manager::ptr tm,
						    superblock_detail::superblock const &sb,
						    nested_output &out) {
		out << "examining top level of mapping tree" << end_message();
		nested_output::nest _ = out.push();

		mapping_reporter mapping_rep(out);
		dev_tree dtree(*tm, sb.data_mapping_root_,
			       mapping_tree_detail::mtree_traits::ref_counter(*tm));
		check_mapping_tree(dtree, mapping_rep);

		return mapping_rep.get_error();
	}

	error_state examine_mapping_tree_(transaction_manager::ptr tm,
					  superblock_detail::superblock const &sb,
					  nested_output &out) {
		out << "examining mapping tree" << end_message();
		nested_output::nest _ = out.push();

		mapping_reporter mapping_rep(out);
		mapping_tree mtree(*tm, sb.data_mapping_root_,
				   mapping_tree_detail::block_traits::ref_counter(tm->get_sm()));
		check_mapping_tree(mtree, mapping_rep);

		return mapping_rep.get_error();
	}

	error_state examine_top_level_mapping_tree(transaction_manager::ptr tm,
						   superblock_detail::superblock const &sb,
						   nested_output &out) {
		error_state err = examine_devices_tree_(tm, sb, out);
		err << examine_top_level_mapping_tree_(tm, sb, out);

		return err;
	}

	error_state examine_mapping_tree(transaction_manager::ptr tm,
					 superblock_detail::superblock const &sb,
					 nested_output &out) {
		error_state err = examine_devices_tree_(tm, sb, out);
		err << examine_mapping_tree_(tm, sb, out);

		return err;
	}

	error_state check_space_map_counts(transaction_manager::ptr tm,
					   superblock_detail::superblock const &sb,
					   nested_output &out) {
		out << "checking space map counts" << end_message();
		nested_output::nest _ = out.push();

		block_counter bc;
		count_metadata(tm, sb, bc);

		// Finally we need to check the metadata space map agrees
		// with the counts we've just calculated.
		error_state err = NO_ERROR;
		persistent_space_map::ptr metadata_sm =
			open_metadata_sm(*tm, static_cast<void const*>(&sb.metadata_space_map_root_));
		for (unsigned b = 0; b < metadata_sm->get_nr_blocks(); b++) {
			ref_t c_actual = metadata_sm->get_count(b);
			ref_t c_expected = bc.get_count(b);

			if (c_actual != c_expected) {
				out << "metadata reference counts differ for block " << b
				    << ", expected " << c_expected
				    << ", but got " << c_actual
				    << end_message();

				err << (c_actual > c_expected ? NON_FATAL : FATAL);
			}
		}

		return err;
	}

	void print_info(transaction_manager::ptr tm,
			superblock_detail::superblock const &sb,
                        nested_output &out)
	{
		out << "TRANSACTION_ID=" << sb.trans_id_ << "\n"
		    << "METADATA_FREE_BLOCKS=" << tm->get_sm()->get_nr_free()
		    << end_message();
	}

	block_address mapping_root(superblock_detail::superblock const &sb, check_options const &opts)
	{
		return opts.override_mapping_root_ ? *opts.override_mapping_root_ : sb.data_mapping_root_;
	}

	//--------------------------------

	class base_metadata_checker : public metadata_checker {
	public:
		base_metadata_checker(block_manager<>::ptr bm,
				      check_options check_opts,
				      output_options output_opts)
			: bm_(bm),
			  options_(check_opts),
			  out_(cerr, 2),
			  info_out_(cout, 0) {

			if (output_opts == OUTPUT_QUIET) {
				out_.disable();
				info_out_.disable();
			}
		}

		error_state check() {
			error_state err = NO_ERROR;

			err << examine_superblock(bm_, out_);

			if (err == FATAL) {
				if (check_for_xml(bm_))
					out_ << "This looks like XML.  thin_check only checks the binary metadata format." << end_message();
				return err;
			}

			superblock_detail::superblock sb = read_superblock(bm_);
			transaction_manager::ptr tm =
				open_tm(bm_, superblock_detail::SUPERBLOCK_LOCATION);
			sb.data_mapping_root_ = mapping_root(sb, options_);

			print_info(tm, sb, info_out_);

			err << examine_data_mappings(tm, sb, options_.check_data_mappings_, out_);

			// if we're checking everything, and there were no errors,
			// then we should check the space maps too.
			if (err != FATAL)
				err << examine_metadata_space_map(tm, sb, options_.check_metadata_space_map_, out_);

			return err;
		}

	private:
		static error_state
		examine_data_mappings(transaction_manager::ptr tm,
				      superblock_detail::superblock const &sb,
				      check_options::data_mapping_options option,
				      nested_output &out) {
			error_state err = NO_ERROR;

			switch (option) {
			case check_options::DATA_MAPPING_LEVEL1:
				err << examine_top_level_mapping_tree(tm, sb, out);
				break;
			case check_options::DATA_MAPPING_LEVEL2:
				err << examine_mapping_tree(tm, sb, out);
				break;
			default:
				break; // do nothing
			}

			return err;
		}

		static error_state
		examine_metadata_space_map(transaction_manager::ptr tm,
					   superblock_detail::superblock const &sb,
					   check_options::metadata_space_map_options option,
					   nested_output &out) {
			error_state err = NO_ERROR;

			switch (option) {
			case check_options::METADATA_SPACE_MAP_FULL:
				err << check_space_map_counts(tm, sb, out);
				break;
			default:
				break; // do nothing
			}

			return err;
		}

		block_manager<>::ptr bm_;
		check_options options_;
		nested_output out_;
		nested_output info_out_;
	};
}

//----------------------------------------------------------------

check_options::check_options()
	: check_data_mappings_(DATA_MAPPING_LEVEL2),
	  check_metadata_space_map_(METADATA_SPACE_MAP_FULL) {
}

void check_options::set_superblock_only() {
	check_data_mappings_ = DATA_MAPPING_NONE;
	check_metadata_space_map_ = METADATA_SPACE_MAP_NONE;
}

void check_options::set_skip_mappings() {
	check_data_mappings_ = DATA_MAPPING_LEVEL1;
	check_metadata_space_map_ = METADATA_SPACE_MAP_NONE;
}

void check_options::set_override_mapping_root(block_address b) {
	override_mapping_root_ = b;
}

metadata_checker::ptr
thin_provisioning::create_base_checker(block_manager<>::ptr bm,
				       check_options const &check_opts,
				       output_options output_opts)
{
	metadata_checker::ptr checker;
	checker = metadata_checker::ptr(new base_metadata_checker(bm, check_opts, output_opts));
	return checker;
}

//----------------------------------------------------------------

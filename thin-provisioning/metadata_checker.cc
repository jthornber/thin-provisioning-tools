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

#include "base/error_state.h"
#include "base/nested_output.h"
#include "persistent-data/file_utils.h"
#include "persistent-data/space-maps/core.h"
#include "thin-provisioning/metadata.h"
#include "thin-provisioning/metadata_checker.h"
#include "thin-provisioning/metadata_counter.h"
#include "thin-provisioning/superblock.h"

using namespace boost;
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

	class data_ref_counter : public mapping_tree_detail::mapping_visitor {
	public:
		data_ref_counter(space_map::ptr sm)
			: sm_(sm) {
		}

		virtual void visit(btree_path const &path, mapping_tree_detail::block_time const &bt) {
			sm_->inc(bt.block_);
		}

        private:
	        space_map::ptr sm_;
	};

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

	error_state examine_superblock(block_manager::ptr bm,
                                       block_address sb_location,
				       nested_output &out) {
		out << "examining superblock" << end_message();
		nested_output::nest _ = out.push();

		superblock_reporter sb_rep(out);
		check_superblock(bm, sb_rep, sb_location);

		return sb_rep.get_error();
	}

	error_state examine_devices_tree_(transaction_manager::ptr tm,
					  superblock_detail::superblock const &sb,
					  nested_output &out,
					  bool ignore_non_fatal) {
		out << "examining devices tree" << end_message();
		nested_output::nest _ = out.push();

		devices_reporter dev_rep(out);
		device_tree dtree(*tm, sb.device_details_root_,
				  device_tree_detail::device_details_traits::ref_counter());
		check_device_tree(dtree, dev_rep, ignore_non_fatal);

		return dev_rep.get_error();
	}

	error_state examine_top_level_mapping_tree_(transaction_manager::ptr tm,
						    superblock_detail::superblock const &sb,
						    nested_output &out,
						    bool ignore_non_fatal) {
		out << "examining top level of mapping tree" << end_message();
		nested_output::nest _ = out.push();

		mapping_reporter mapping_rep(out);
		dev_tree dtree(*tm, sb.data_mapping_root_,
			       mapping_tree_detail::mtree_traits::ref_counter(*tm));
		check_mapping_tree(dtree, mapping_rep, ignore_non_fatal);

		return mapping_rep.get_error();
	}

	error_state examine_mapping_tree_(transaction_manager::ptr tm,
					  superblock_detail::superblock const &sb,
					  nested_output &out,
                                          optional<space_map::ptr> data_sm,
					  bool ignore_non_fatal) {
		out << "examining mapping tree" << end_message();
		nested_output::nest _ = out.push();

		mapping_reporter mapping_rep(out);
		mapping_tree mtree(*tm, sb.data_mapping_root_,
				   mapping_tree_detail::block_traits::ref_counter(tm->get_sm()));

		if (data_sm) {
			data_ref_counter dcounter(*data_sm);
			walk_mapping_tree(mtree, dcounter, mapping_rep, ignore_non_fatal);
		} else
			check_mapping_tree(mtree, mapping_rep, ignore_non_fatal);

		return mapping_rep.get_error();
	}

	error_state examine_top_level_mapping_tree(transaction_manager::ptr tm,
						   superblock_detail::superblock const &sb,
						   nested_output &out,
						   bool ignore_non_fatal) {
		error_state err = examine_devices_tree_(tm, sb, out, ignore_non_fatal);
		err << examine_top_level_mapping_tree_(tm, sb, out, ignore_non_fatal);

		return err;
	}

	error_state examine_mapping_tree(transaction_manager::ptr tm,
					 superblock_detail::superblock const &sb,
					 nested_output &out,
	                                 optional<space_map::ptr> data_sm,
					 bool ignore_non_fatal) {
		error_state err = examine_devices_tree_(tm, sb, out, ignore_non_fatal);
		err << examine_mapping_tree_(tm, sb, out, data_sm, ignore_non_fatal);

		return err;
	}

	error_state compare_metadata_space_maps(space_map::ptr actual,
						block_counter const &expected,
						nested_output &out) {
		error_state err = NO_ERROR;
		block_address nr_blocks = actual->get_nr_blocks();

		for (block_address b = 0; b < nr_blocks; b++) {
			ref_t c_actual = actual->get_count(b);
			ref_t c_expected = expected.get_count(b);

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

	error_state collect_leaked_blocks(space_map::ptr actual,
					  block_counter const &expected,
					  std::set<block_address> &leaked) {
		error_state err = NO_ERROR;
		block_address nr_blocks = actual->get_nr_blocks();

		for (block_address b = 0; b < nr_blocks; b++) {
			ref_t c_actual = actual->get_count(b);
			ref_t c_expected = expected.get_count(b);

			if (c_actual == c_expected)
				continue;

			if (c_actual < c_expected) {
				err << FATAL;
				break;
			}

			// Theoretically, the ref-count of a leaked block
			// should be only one. Here a leaked ref-count of two
			// is allowed.
			if (c_expected || c_actual >= 3)
				err << NON_FATAL;
			else if (c_actual > 0)
				leaked.insert(b);
		}

		return err;
	}

	error_state clear_leaked_blocks(space_map::ptr actual,
					block_counter const &expected) {
		error_state err = NO_ERROR;
		std::set<block_address> leaked;

		err << collect_leaked_blocks(actual, expected, leaked);
		if (err != NO_ERROR)
			return err;

		for (auto const &b : leaked)
			actual->set_count(b, 0);

		return err;
	}

	error_state check_metadata_space_map_counts(transaction_manager::ptr tm,
						    superblock_detail::superblock const &sb,
						    block_counter &bc,
						    nested_output &out) {
		out << "checking space map counts" << end_message();
		nested_output::nest _ = out.push();

		if (!count_metadata(tm, sb, bc))
			return FATAL;

		// Finally we need to check the metadata space map agrees
		// with the counts we've just calculated.
		space_map::ptr metadata_sm =
			open_metadata_sm(*tm, static_cast<void const*>(&sb.metadata_space_map_root_));
		return compare_metadata_space_maps(metadata_sm, bc, out);
	}

	error_state compare_space_maps(space_map::ptr actual, space_map::ptr expected,
                                       nested_output &out)
	{
		error_state err = NO_ERROR;
		auto nr_blocks = actual->get_nr_blocks();

		if (expected->get_nr_blocks() != nr_blocks) {
			out << "internal error: nr blocks in space maps differ"
			    << end_message();
			err << FATAL;
		} else {
			for (block_address b = 0; b < nr_blocks; b++) {
				auto a_count = actual->get_count(b);
				auto e_count = expected->get_count(b);

				if (a_count != e_count) {
					out << "data reference counts differ for block " << b
					    << ", expected " << e_count
					    << ", but got " << a_count
					    << end_message();
					err << (a_count > e_count ? NON_FATAL : FATAL);
				}
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

	class metadata_checker {
	public:
		metadata_checker(std::string const &path,
		      		 check_options check_opts,
				 output_options output_opts)
			: path_(path),
			  options_(check_opts),
			  out_(cerr, 2),
			  info_out_(cout, 0),
			  expected_rc_(true), // set stop on the first error
			  err_(NO_ERROR) {

			if (output_opts == OUTPUT_QUIET) {
				out_.disable();
				info_out_.disable();
			}

			sb_location_ = get_superblock_location();
		}

		void check() {
			block_manager::ptr bm = open_bm(path_, block_manager::READ_ONLY,
							!options_.use_metadata_snap_);

			err_ = examine_superblock(bm, sb_location_, out_);
			if (err_ == FATAL) {
				if (check_for_xml(bm))
					out_ << "This looks like XML.  thin_check only checks the binary metadata format." << end_message();
				return;
			}

			transaction_manager::ptr tm = open_tm(bm, sb_location_);
			superblock_detail::superblock sb = read_superblock(bm, sb_location_);
			sb.data_mapping_root_ = mapping_root(sb, options_);

			print_info(tm, sb, info_out_);

			if (options_.sm_opts_ == check_options::SPACE_MAP_FULL) {
				// data block reference counting is disabled
				// until that there's a better solution in space
				// and time complexity
				space_map::ptr data_sm{open_disk_sm(*tm, &sb.data_space_map_root_)};
				optional<space_map::ptr> core_sm;
				err_ << examine_data_mappings(tm, sb, options_.data_mapping_opts_, out_, core_sm);

				if (err_ == FATAL)
					return;

				// if we're checking everything, and there were no errors,
				// then we should check the space maps too.
				err_ << examine_metadata_space_map(tm, sb, options_.sm_opts_, out_, expected_rc_);

				// verify ref-counts of data blocks
				if (err_ != FATAL && core_sm)
					err_ << compare_space_maps(data_sm, *core_sm, out_);
			} else
				err_ << examine_data_mappings(tm, sb, options_.data_mapping_opts_, out_,
							      optional<space_map::ptr>());
		}

		bool fix_metadata_leaks() {
			if (!verify_preconditions_before_fixing()) {
				out_ << "metadata has not been fully examined" << end_message();
				return false;
			}

			// skip if the metadata cannot be fixed, or there's no leaked blocks
			if (err_ == FATAL)
				return false;
			else if (err_ == NO_ERROR)
				return true;

			block_manager::ptr bm = open_bm(path_, block_manager::READ_WRITE);
			superblock_detail::superblock sb = read_superblock(bm, sb_location_);
			transaction_manager::ptr tm = open_tm(bm, sb_location_);
			persistent_space_map::ptr metadata_sm =
				open_metadata_sm(*tm, static_cast<void const*>(&sb.metadata_space_map_root_));
			tm->set_sm(metadata_sm);

			err_ = clear_leaked_blocks(metadata_sm, expected_rc_);

			if (err_ != NO_ERROR)
				return false;

			metadata_sm->commit();
			metadata_sm->copy_root(&sb.metadata_space_map_root_, sizeof(sb.metadata_space_map_root_));
			write_superblock(bm, sb);

			out_ << "fixed metadata leaks" << end_message();

			return true;
		}

		bool clear_needs_check_flag() {
			if (!verify_preconditions_before_fixing()) {
				out_ << "metadata has not been fully examined" << end_message();
				return false;
			}

			if (err_ != NO_ERROR)
				return false;

			block_manager::ptr bm = open_bm(path_, block_manager::READ_WRITE);
			superblock_detail::superblock sb = read_superblock(bm);

			if (!sb.get_needs_check_flag())
				return true;

			sb.set_needs_check_flag(false);
			write_superblock(bm, sb);

			out_ << "cleared needs_check flag" << end_message();

			return true;
		}

		bool get_status() const {
			if (options_.ignore_non_fatal_)
				return (err_ == FATAL) ? false : true;

			return (err_ == NO_ERROR) ? true : false;
		}

	private:
		block_address
		get_superblock_location() {
			block_address sb_location = superblock_detail::SUPERBLOCK_LOCATION;

			if (options_.use_metadata_snap_) {
				block_manager::ptr bm = open_bm(path_, block_manager::READ_ONLY,
								!options_.use_metadata_snap_);
				superblock_detail::superblock sb = read_superblock(bm, sb_location);
				sb_location = sb.metadata_snap_;
				if (sb_location == superblock_detail::SUPERBLOCK_LOCATION)
					throw runtime_error("No metadata snapshot found.");
			}

			return sb_location;
		}

		error_state
		examine_data_mappings(transaction_manager::ptr tm,
				      superblock_detail::superblock const &sb,
				      check_options::data_mapping_options option,
				      nested_output &out,
                                      optional<space_map::ptr> data_sm) {
			error_state err = NO_ERROR;

			switch (option) {
			case check_options::DATA_MAPPING_LEVEL1:
				err << examine_top_level_mapping_tree(tm, sb, out, options_.ignore_non_fatal_);
				break;
			case check_options::DATA_MAPPING_LEVEL2:
				err << examine_mapping_tree(tm, sb, out, data_sm, options_.ignore_non_fatal_);
				break;
			default:
				break; // do nothing
			}

			return err;
		}

		static error_state
		examine_metadata_space_map(transaction_manager::ptr tm,
					   superblock_detail::superblock const &sb,
					   check_options::space_map_options option,
					   nested_output &out,
					   block_counter &bc) {
			error_state err = NO_ERROR;

			switch (option) {
			case check_options::SPACE_MAP_FULL:
				err << check_metadata_space_map_counts(tm, sb, bc, out);
				break;
			default:
				break; // do nothing
			}

			return err;
		}

		bool verify_preconditions_before_fixing() const {
			if (options_.use_metadata_snap_ ||
			    !!options_.override_mapping_root_ ||
			    options_.sm_opts_ != check_options::SPACE_MAP_FULL ||
			    options_.data_mapping_opts_ != check_options::DATA_MAPPING_LEVEL2)
				return false;

			if (!expected_rc_.get_counts().size())
				return false;

			return true;
		}

		std::string const &path_;
		check_options options_;
		nested_output out_;
		nested_output info_out_;
		block_address sb_location_;
		block_counter expected_rc_;
		base::error_state err_; // metadata state
	};
}

//----------------------------------------------------------------

check_options::check_options()
	: use_metadata_snap_(false),
	  data_mapping_opts_(DATA_MAPPING_LEVEL2),
	  sm_opts_(SPACE_MAP_FULL),
	  ignore_non_fatal_(false),
	  fix_metadata_leaks_(false),
	  clear_needs_check_(false) {
}

void check_options::set_superblock_only() {
	data_mapping_opts_ = DATA_MAPPING_NONE;
	sm_opts_ = SPACE_MAP_NONE;
}

void check_options::set_skip_mappings() {
	data_mapping_opts_ = DATA_MAPPING_LEVEL1;
	sm_opts_ = SPACE_MAP_NONE;
}

void check_options::set_override_mapping_root(block_address b) {
	override_mapping_root_ = b;
}

void check_options::set_metadata_snap() {
	use_metadata_snap_ = true;
	sm_opts_ = SPACE_MAP_NONE;
}

void check_options::set_ignore_non_fatal() {
	ignore_non_fatal_ = true;
}

void check_options::set_fix_metadata_leaks() {
	fix_metadata_leaks_ = true;
}

void check_options::set_clear_needs_check() {
	clear_needs_check_ = true;
}

bool check_options::check_conformance() {
	if (fix_metadata_leaks_ || clear_needs_check_) {
		if (ignore_non_fatal_) {
			cerr << "cannot perform fix by ignoring non-fatal errors" << endl;
			return false;
		}

		if (use_metadata_snap_) {
			cerr << "cannot perform fix within metadata snap" << endl;
			return false;
		}

		if (!!override_mapping_root_) {
			cerr << "cannot perform fix with an overridden mapping root" << endl;
			return false;
		}

		if (data_mapping_opts_ != DATA_MAPPING_LEVEL2 ||
		    sm_opts_ != SPACE_MAP_FULL) {
			cerr << "cannot perform fix without a full examination" << endl;
			return false;
		}
	}

	return true;
}

//----------------------------------------------------------------

bool
thin_provisioning::check_metadata(std::string const &path,
				  check_options const &check_opts,
				  output_options output_opts)
{
	metadata_checker checker(path, check_opts, output_opts);

	checker.check();
	if (check_opts.fix_metadata_leaks_)
		checker.fix_metadata_leaks();
	if (check_opts.fix_metadata_leaks_ ||
	    check_opts.clear_needs_check_)
		checker.clear_needs_check_flag();

	return checker.get_status();
}

//----------------------------------------------------------------

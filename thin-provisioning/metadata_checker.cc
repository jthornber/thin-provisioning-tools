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
				auto e_count = actual->get_count(b);
				
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
		metadata_checker(block_manager::ptr bm,
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
			auto sb_location = superblock_detail::SUPERBLOCK_LOCATION;

			if (options_.use_metadata_snap_) {
				superblock_detail::superblock sb = read_superblock(bm_, sb_location);
				sb_location = sb.metadata_snap_;
				if (sb_location == superblock_detail::SUPERBLOCK_LOCATION)
					throw runtime_error("No metadata snapshot found.");
			}
			
			err << examine_superblock(bm_, sb_location, out_);
			if (err == FATAL) {
				if (check_for_xml(bm_))
					out_ << "This looks like XML.  thin_check only checks the binary metadata format." << end_message();
				return err;
			}

			superblock_detail::superblock sb = read_superblock(bm_, sb_location);
			transaction_manager::ptr tm = open_tm(bm_, sb_location);
			sb.data_mapping_root_ = mapping_root(sb, options_);

			print_info(tm, sb, info_out_);

			if (options_.sm_opts_ == check_options::SPACE_MAP_FULL) {
				space_map::ptr data_sm{open_disk_sm(*tm, &sb.data_space_map_root_)};
				optional<space_map::ptr> core_sm{create_core_map(data_sm->get_nr_blocks())};
				err << examine_data_mappings(tm, sb, options_.check_data_mappings_, out_, core_sm);

				// if we're checking everything, and there were no errors,
				// then we should check the space maps too.
				if (err != FATAL) {
					err << examine_metadata_space_map(tm, sb, options_.sm_opts_, out_);

					if (core_sm)
						err << compare_space_maps(data_sm, *core_sm, out_);
				}
			} else
				err << examine_data_mappings(tm, sb, options_.check_data_mappings_, out_,
                                                             optional<space_map::ptr>());

			return err;
		}

	private:
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
					   nested_output &out) {
			error_state err = NO_ERROR;

			switch (option) {
			case check_options::SPACE_MAP_FULL:
				err << check_space_map_counts(tm, sb, out);
				break;
			default:
				break; // do nothing
			}

			return err;
		}

		block_manager::ptr bm_;
		check_options options_;
		nested_output out_;
		nested_output info_out_;
	};
}

//----------------------------------------------------------------

check_options::check_options()
	: use_metadata_snap_(false),
	  check_data_mappings_(DATA_MAPPING_LEVEL2),
	  sm_opts_(SPACE_MAP_FULL),
	  ignore_non_fatal_(false) {
}

void check_options::set_superblock_only() {
	check_data_mappings_ = DATA_MAPPING_NONE;
	sm_opts_ = SPACE_MAP_NONE;
}

void check_options::set_skip_mappings() {
	check_data_mappings_ = DATA_MAPPING_LEVEL1;
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
		
base::error_state
thin_provisioning::check_metadata(block_manager::ptr bm,
				  check_options const &check_opts,
				  output_options output_opts)
{
	metadata_checker checker(bm, check_opts, output_opts);
	return checker.check();
}

//----------------------------------------------------------------

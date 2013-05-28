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

#ifndef METADATA_CHECKER_H
#define METADATA_CHECKER_H

#include "persistent-data/block.h"
#include "persistent-data/error_set.h"
#include "persistent-data/run.h"
#include "persistent-data/space_map.h"

#include <deque>

//----------------------------------------------------------------

namespace thin_provisioning {
	// FIXME: should take a block manager or transaction manager
	void check_metadata(std::string const &path);









#if 0

	// Base class for all types of metadata damage.  Used in reporting.
	class metadata_damage {
	public:
		typedef boost::shared_ptr<metadata_damage> ptr;
		virtual ~metadata_damage() {}
		virtual void visit(metadata_damage_visitor &visitor) const = 0;

		void set_message(std::string const &message);
		std::string const &get_message() const;

	private:
		std::string message_;
	};

	// FIXME: there's a mix of abstraction here, some classes represent
	// the actual damage on disk (bad_ref_count), others represent the
	// repercussions (missing_mapping).  Need to revist, probably once
	// we've got the reporting layer in.

	class super_block_corruption : public metadata_damage {
		void visit(metadata_damage_visitor &visitor) const;
		bool operator ==(super_block_corruption const &rhs) const;
	};

	typedef base::run<uint64_t> run64;

	struct missing_device_details : public metadata_damage {
		missing_device_details(run64 missing);
		virtual void visit(metadata_damage_visitor &visitor) const;
		bool operator ==(missing_device_details const &rhs) const;

		run64 missing_;
	};

	struct missing_devices : public metadata_damage {
		missing_devices(run64 missing);
		virtual void visit(metadata_damage_visitor &visitor) const;
		bool operator ==(missing_devices const &rhs) const;

		run64 missing_;
	};

	struct missing_mappings : public metadata_damage {
		missing_mappings(uint64_t dev, run64 missing);
		virtual void visit(metadata_damage_visitor &visitor) const;
		bool operator ==(missing_mappings const &rhs) const;

		uint64_t dev_;
		run64 missing_;
	};

	struct bad_metadata_ref_count : public metadata_damage {
		bad_metadata_ref_count(block_address b,
				       ref_t actual,
				       ref_t expected);

		virtual void visit(metadata_damage_visitor &visitor) const;
		bool operator ==(bad_metadata_ref_count const &rhs) const;

		block_address b_;
		ref_t actual_;
		ref_t expected_;
	};

	struct bad_data_ref_count : public metadata_damage {
		bad_data_ref_count(block_address b,
				   ref_t actual,
				   ref_t expected);

		virtual void visit(metadata_damage_visitor &visitor) const;
		bool operator ==(bad_data_ref_count const &rhs) const;

		block_address b_;
		ref_t actual_;
		ref_t expected_;
	};

	struct missing_metadata_ref_counts : public metadata_damage {
		missing_metadata_ref_counts(run64 missing);
		virtual void visit(metadata_damage_visitor &visitor) const;
		bool operator ==(missing_metadata_ref_counts const &rhs) const;

		run64 missing_;
	};

	struct missing_data_ref_counts : public metadata_damage {
		missing_data_ref_counts(run64 missing);
		virtual void visit(metadata_damage_visitor &visitor) const;
		bool operator ==(missing_data_ref_counts const &rhs) const;

		run64 missing_;
	};

	class metadata_damage_visitor {
	public:
		typedef boost::shared_ptr<metadata_damage_visitor> ptr;

		virtual ~metadata_damage_visitor() {}

		void visit(metadata_damage const &damage);
		virtual void visit(super_block_corruption const &damage) = 0;
		virtual void visit(missing_device_details const &damage) = 0;
		virtual void visit(missing_devices const &damage) = 0;
		virtual void visit(missing_mappings const &damage) = 0;
		virtual void visit(bad_metadata_ref_count const &damage) = 0;
		virtual void visit(bad_data_ref_count const &damage) = 0;
		virtual void visit(missing_metadata_ref_counts const &damage) = 0;
		virtual void visit(missing_data_ref_counts const &damage) = 0;
	};

	typedef std::deque<metadata_damage::ptr> damage_list;
	typedef boost::shared_ptr<damage_list> damage_list_ptr;
#endif
}

//----------------------------------------------------------------

#endif

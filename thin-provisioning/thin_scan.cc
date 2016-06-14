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

#include <boost/lexical_cast.hpp>
#include <boost/optional.hpp>
#include <getopt.h>
#include <vector>
#include <fstream>

#include "persistent-data/data-structures/btree.h"
#include "persistent-data/data-structures/simple_traits.h"
#include "persistent-data/file_utils.h"
#include "persistent-data/space-maps/core.h"
#include "persistent-data/space-maps/disk_structures.h"
#include "thin-provisioning/metadata.h"
#include "thin-provisioning/superblock.h"
#include "thin-provisioning/commands.h"
#include "version.h"

using namespace thin_provisioning;

//----------------------------------------------------------------

namespace {
	// extracted from btree_damage_visitor.h
	template <typename node>
	bool check_block_nr(node const &n) {
		if (n.get_location() != n.get_block_nr()) {
			return false;
		}
		return true;
	}

	// extracted from btree_damage_visitor.h
	template <typename node>
	bool check_max_entries(node const &n) {
		size_t elt_size = sizeof(uint64_t) + n.get_value_size();
		if (elt_size * n.get_max_entries() + sizeof(node_header) > MD_BLOCK_SIZE) {
			return false;
		}

		if (n.get_max_entries() % 3) {
			return false;
		}

		return true;
	}

	// extracted from btree_damage_visitor.h
	template <typename node>
	bool check_nr_entries(node const &n, bool is_root) {
		if (n.get_nr_entries() > n.get_max_entries()) {
			return false;
		}

		block_address min = n.get_max_entries() / 3;
		if (!is_root && (n.get_nr_entries() < min)) {
			return false;
		}

		return true;
	}

	// extracted from btree_damage_visitor.h
	template <typename node>
	bool check_ordered_keys(node const &n) {
		unsigned nr_entries = n.get_nr_entries();

		if (nr_entries == 0)
			return true; // can only happen if a root node

		uint64_t last_key = n.key_at(0);

		for (unsigned i = 1; i < nr_entries; i++) {
			uint64_t k = n.key_at(i);
			if (k <= last_key) {
				return false;
			}
			last_key = k;
		}

		return true;
	}

	transaction_manager::ptr
	open_tm(block_manager<>::ptr bm) {
		space_map::ptr sm(new core_map(bm->get_nr_blocks()));
		sm->inc(superblock_detail::SUPERBLOCK_LOCATION);
		transaction_manager::ptr tm(new transaction_manager(bm, sm));
		return tm;
	}
}

namespace {
	// FIXME: deprecated conversion from string constant to ‘char*’
	char const* metadata_block_type_name[] = {
		"unknown",
		"zero",
		"superblock",
		"btree_internal",
		"btree_leaf",
		"btree_unknown",
		"index_block",
		"bitmap_block"
	};

	enum metadata_block_type {
		UNKNOWN = 0,
		ZERO,
		SUPERBLOCK,
		BTREE_INTERNAL,
		BTREE_LEAF,
		BTREE_UNKNOWN,
		INDEX_BLOCK,
		BITMAP_BLOCK
	};

	struct block_range {
		block_range()
			: begin_(0), end_(0),
			  type_(UNKNOWN), ref_count_(-1),
			  value_size_(0), is_valid_(false)
		{
		}

		block_range(block_range const &rhs)
			: begin_(rhs.begin_), end_(rhs.end_),
			  blocknr_begin_(rhs.blocknr_begin_),
			  type_(rhs.type_), ref_count_(rhs.ref_count_),
			  value_size_(rhs.value_size_), is_valid_(rhs.is_valid_)
		{
		}

		uint64_t size() const {
			return (end_ > begin_) ? (end_ - begin_) : 0;
		}

		// returns true if r is left or right-adjacent
		bool is_adjacent_to(block_range const &r) const {
			block_range const &lhs = begin_ < r.begin_ ? *this : r;
			block_range const &rhs = begin_ < r.begin_ ? r : *this;

			if (size() && r.size() &&
			    rhs.begin_ == lhs.end_ &&
			    ((!blocknr_begin_ && !r.blocknr_begin_) ||
			     (blocknr_begin_ && r.blocknr_begin_ &&
			      *rhs.blocknr_begin_ >= *lhs.blocknr_begin_ &&
			      (*rhs.blocknr_begin_ - *lhs.blocknr_begin_ == rhs.begin_ - lhs.begin_))) &&
			    type_ == r.type_ &&
			    ref_count_ == r.ref_count_ &&
			    value_size_ == r.value_size_ &&
			    is_valid_ == r.is_valid_)
				return true;

			return false;
		}

		bool concat(block_range const &r) {
			if (!is_adjacent_to(r))
				return false;
			begin_ = std::min(begin_, r.begin_);
			end_ = std::max(end_, r.end_);
			return true;
		}

		uint64_t begin_;
		uint64_t end_; // one-pass-the-end
		boost::optional<uint64_t> blocknr_begin_;
		metadata_block_type type_;
		int64_t ref_count_; // ref_count in metadata space map
		size_t value_size_; // btree node only
		bool is_valid_;
	};

	void output_block_range(block_range const &r, std::ostream &out) {
		if (!r.size())
			return;

		if (r.end_ - r.begin_ > 1) {
			out << "<range_block type=\"" << metadata_block_type_name[r.type_]
			    << "\" location_begin=\"" << r.begin_;
			if (r.blocknr_begin_)
			    out << "\" blocknr_begin=\"" << *r.blocknr_begin_;
			out << "\" length=\"" << r.end_ - r.begin_
			    << "\" ref_count=\"" << r.ref_count_
			    << "\" is_valid=\"" << r.is_valid_;
		} else {
			out << "<single_block type=\"" << metadata_block_type_name[r.type_]
			    << "\" location=\"" << r.begin_;
			if (r.blocknr_begin_)
			    out << "\" blocknr=\"" << *r.blocknr_begin_;
			out << "\" ref_count=\"" << r.ref_count_
			    << "\" is_valid=\"" << r.is_valid_;
		}

		if (r.type_ == BTREE_INTERNAL || r.type_ == BTREE_LEAF || r.type_ == BTREE_UNKNOWN) {
			out << "\" value_size=\"" << r.value_size_ << "\"/>" << endl;
		} else
			out << "\"/>" << endl;
	}

	//-------------------------------------------------------------------

	struct flags {
		flags() {
		}

		boost::optional<block_address> scan_begin_;
		boost::optional<block_address> scan_end_;
	};

	int scan_metadata_(string const &input,
			   std::ostream &out,
			   flags const &f) {
		using namespace persistent_data;
		using namespace thin_provisioning;
		using namespace sm_disk_detail;

		block_manager<>::ptr bm;
		bm = open_bm(input, block_manager<>::READ_ONLY);

		block_address scan_begin = f.scan_begin_ ? *f.scan_begin_ : 0;
		block_address scan_end = f.scan_end_ ? *f.scan_end_ : bm->get_nr_blocks();

		const std::vector<char> zeros(MD_BLOCK_SIZE, 0);

		// try to open metadata space-map (it's okay to fail)
		// note: transaction_manager and space_map must be in the same scope
		transaction_manager::ptr tm;
		checked_space_map::ptr metadata_sm;
		try {
			superblock_detail::superblock sb = read_superblock(bm);
			tm = open_tm(bm);
			metadata_sm = open_metadata_sm(*tm, &sb.metadata_space_map_root_);
			tm->set_sm(metadata_sm);
		} catch (std::exception &e) {
			cerr << e.what() << endl;
		}

		block_range curr_range;
		block_range run_range;

		bcache::validator::ptr sv = superblock_validator();
		bcache::validator::ptr nv = create_btree_node_validator();
		bcache::validator::ptr iv = index_validator();
		bcache::validator::ptr bv = bitmap_validator();

		for (block_address b = scan_begin; b < scan_end; ++b) {
			block_manager<>::read_ref rr = bm->read_lock(b);

			curr_range.begin_ = b;
			curr_range.end_ = b + 1;
			curr_range.blocknr_begin_ = boost::none;
			curr_range.type_ = UNKNOWN;
			curr_range.is_valid_ = false;

			if (!memcmp(rr.data(), zeros.data(), MD_BLOCK_SIZE))
				curr_range.type_ = ZERO;

			if (curr_range.type_ == UNKNOWN && sv->check_raw(rr.data())) {
				curr_range.type_ = SUPERBLOCK;
				curr_range.is_valid_ = true;
			}

			if (curr_range.type_ == UNKNOWN && nv->check_raw(rr.data())) {
				// note: check_raw() doesn't check node_header::blocknr_
				node_ref<uint64_traits> n = btree_detail::to_node<uint64_traits>(rr);
				uint32_t flags = to_cpu<uint32_t>(n.raw()->header.flags);
				if ((flags & INTERNAL_NODE) && !(flags & LEAF_NODE))
					curr_range.type_ = BTREE_INTERNAL;
				else if (flags & LEAF_NODE)
					curr_range.type_ = BTREE_LEAF;
				else
					curr_range.type_ = BTREE_UNKNOWN;

				if (curr_range.type_ != BTREE_UNKNOWN &&
				    check_block_nr(n) &&
				    check_max_entries(n) &&
				    check_nr_entries(n, true) &&
				    check_ordered_keys(n))
					curr_range.is_valid_ = true;
				else
					curr_range.is_valid_ = false;

				curr_range.blocknr_begin_ = n.get_block_nr();
				curr_range.value_size_ = n.get_value_size();
			}

			if (curr_range.type_ == UNKNOWN && bv->check_raw(rr.data())) {
				curr_range.type_ = BITMAP_BLOCK;
				bitmap_header const *data = reinterpret_cast<bitmap_header const *>(rr.data());
				curr_range.blocknr_begin_ = to_cpu<uint64_t>(data->blocknr);
				curr_range.is_valid_ = (to_cpu<uint64_t>(data->blocknr) == b) ? true : false;
			}

			if (curr_range.type_ == UNKNOWN && iv->check_raw(rr.data())) {
				curr_range.type_ = INDEX_BLOCK;
				metadata_index const *mi = reinterpret_cast<metadata_index const *>(rr.data());
				curr_range.blocknr_begin_ = to_cpu<uint64_t>(mi->blocknr_);
				curr_range.is_valid_ = (to_cpu<uint64_t>(mi->blocknr_) == b) ? true : false;
			}

			try {
				curr_range.ref_count_ = metadata_sm ?
							static_cast<int64_t>(metadata_sm->get_count(b)) : -1;
			} catch (std::exception &e) {
				curr_range.ref_count_ = -1;
			}

			// store the current block
			if (!run_range.concat(curr_range)) {
				output_block_range(run_range, out);
				run_range = curr_range;
			}
		}

		// output the last run
		output_block_range(run_range, out);

		return 0;
	}

	int scan_metadata(string const &input,
			  boost::optional<string> output,
			  flags const &f) {
		try {
			if (output) {
				std::ofstream out(output->c_str());
				scan_metadata_(input, out, f);
			} else
				scan_metadata_(input, cout, f);
		} catch (std::exception &e) {
			cerr << e.what() << endl;
			return 1;
		}
		return 0;
	}
}

//---------------------------------------------------------------------------

thin_scan_cmd::thin_scan_cmd()
	: command("thin_scan")
{
}

void
thin_scan_cmd::usage(std::ostream &out) const {
	out << "Usage: " << get_name() << " [options] {device|file}" << endl
	    << "Options:" << endl
	    << "  {-h|--help}" << endl
	    << "  {-o|--output} <xml file>" << endl
	    << "  {--begin} <block#>" << endl
	    << "  {--end} <block#>" << endl
	    << "  {-V|--version}" << endl;
}

int
thin_scan_cmd::run(int argc, char **argv)
{
	const char shortopts[] = "ho:V";
	const struct option longopts[] = {
		{ "help", no_argument, NULL, 'h'},
		{ "output", required_argument, NULL, 'o'},
		{ "version", no_argument, NULL, 'V'},
		{ "begin", required_argument, NULL, 1},
		{ "end", required_argument, NULL, 2},
		{ NULL, no_argument, NULL, 0 }
	};
	boost::optional<string> output;
	flags f;

	int c;
	while ((c = getopt_long(argc, argv, shortopts, longopts, NULL)) != -1) {
		switch(c) {
		case 'h':
			usage(cout);
			return 0;

		case 'o':
			output = optarg;
			break;

		case 'V':
			cout << THIN_PROVISIONING_TOOLS_VERSION << endl;
			return 0;

		case 1:
			try {
				f.scan_begin_ = boost::lexical_cast<uint64_t>(optarg);
			} catch (std::exception &e) {
				cerr << e.what() << endl;
				return 1;
			}
			break;

		case 2:
			try {
				f.scan_end_ = boost::lexical_cast<uint64_t>(optarg);
			} catch (std::exception &e) {
				cerr << e.what() << endl;
				return 1;
			}
			break;

		default:
			usage(cerr);
			return 1;
		}
	}

	if (argc == optind) {
		cerr << "No input file provided." << endl;
		usage(cerr);
		return 1;
	}

	if (f.scan_begin_ && f.scan_end_ && (*f.scan_end_ <= *f.scan_begin_)) {
		cerr << "badly formed region (end <= begin)" << endl;
		return 1;
	}

	return scan_metadata(argv[optind], output, f);
}

//---------------------------------------------------------------------------

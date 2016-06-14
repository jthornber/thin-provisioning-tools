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

#include "base/indented_stream.h"
#include "persistent-data/file_utils.h"
#include "persistent-data/data-structures/btree.h"
#include "persistent-data/data-structures/btree_counter.h"
#include "persistent-data/data-structures/btree_damage_visitor.h"
#include "persistent-data/data-structures/btree_node_checker.h"
#include "persistent-data/data-structures/simple_traits.h"
#include "persistent-data/space-maps/core.h"
#include "persistent-data/space-maps/disk_structures.h"
#include "thin-provisioning/metadata.h"
#include "thin-provisioning/metadata_counter.h"
#include "thin-provisioning/commands.h"
#include "version.h"

using namespace thin_provisioning;
using namespace persistent_data;

//----------------------------------------------------------------

namespace {
	transaction_manager::ptr
	open_tm(block_manager<>::ptr bm) {
		space_map::ptr sm(new core_map(bm->get_nr_blocks()));
		sm->inc(superblock_detail::SUPERBLOCK_LOCATION);
		transaction_manager::ptr tm(new transaction_manager(bm, sm));
		return tm;
	}
}

//---------------------------------------------------------------------------

namespace {
	struct node_info {
		uint64_t blocknr_;
		uint32_t flags_;
		uint64_t key_begin_;
		uint64_t key_end_;
		uint64_t nr_entries_;
		uint32_t value_size_;
	};

	//-------------------------------------------------------------------

	struct btree_node_checker {
		typedef boost::shared_ptr<btree_node_checker> ptr;
		virtual ~btree_node_checker() {}
		virtual bool check(node_ref<uint64_traits> &n) = 0;
	};

	struct unvisited_btree_node_filter: public btree_node_checker {
		unvisited_btree_node_filter(block_counter const &bc)
			: nv_(create_btree_node_validator()), bc_(bc) {
		}

		virtual bool check(node_ref<uint64_traits> &n) {
			uint32_t flags = to_cpu<uint32_t>(n.raw()->header.flags);
			if ((n.get_value_size() == sizeof(mapping_tree_detail::block_traits::disk_type) ||
			     n.get_value_size() == sizeof(device_tree_detail::device_details_traits::disk_type)) &&
			    !bc_.get_count(n.get_location()) &&
			    checker_.check_block_nr(n) &&
			    (((flags & INTERNAL_NODE) && !(flags & LEAF_NODE)) ||
			     (flags & LEAF_NODE)) &&
			    nv_->check_raw(n.raw()) &&
			    checker_.check_max_entries(n) &&
			    checker_.check_nr_entries(n, true) &&
			    checker_.check_ordered_keys(n))
				return true;
			return false;
		}

		bcache::validator::ptr nv_;
		block_counter const &bc_;
		btree_detail::btree_node_checker checker_;
	};

	//-------------------------------------------------------------------

	void find_btree_nodes(block_manager<>::ptr bm,
			      block_address begin,
			      block_address end,
			      btree_node_checker::ptr checker,
			      base::run_set<block_address> &found) {
		using namespace persistent_data;

		for (block_address b = begin; b < end; ++b) {
			block_manager<>::read_ref rr = bm->read_lock(b);
			node_ref<uint64_traits> n = btree_detail::to_node<uint64_traits>(rr);

			if (checker->check(n))
				found.add(b);
		}
	}

	//-------------------------------------------------------------------

	bool first_key_cmp(node_info const &lhs, node_info const &rhs) {
		return lhs.key_begin_ < rhs.key_begin_;
	}

	template <typename ValueTraits>
	void convert_to_node_info(node_ref<ValueTraits> const &n, node_info &ni) {
		ni.blocknr_ = n.get_location();
		ni.flags_ = to_cpu<uint32_t>(n.raw()->header.flags);
		if ((ni.nr_entries_ = n.get_nr_entries()) > 0) {
			ni.key_begin_ = n.key_at(0);
			ni.key_end_ = n.key_at(n.get_nr_entries() - 1);
		}
		ni.value_size_ = n.get_value_size();
	}

	void output_node_info(indented_stream &out, node_info const &ni) {
		out.indent();
		out << "<node blocknr=\"" << ni.blocknr_
		    << "\" flags=\"" << ni.flags_
		    << "\" key_begin=\"" << ni.key_begin_
		    << "\" key_end=\"" << ni.key_end_
		    << "\" nr_entries=\"" << ni.nr_entries_
		    << "\" value_size=\"" << ni.value_size_
		    << "\"/>" << endl;
	}

	//-------------------------------------------------------------------

	class ll_mapping_tree_emitter : public mapping_tree_detail::device_visitor {
	public:
		ll_mapping_tree_emitter(block_manager<>::ptr bm,
					indented_stream &out)
			: bm_(bm), out_(out) {
		}

		void visit(btree_path const &path, block_address tree_root) {
			out_.indent();
			out_ << "<device dev_id=\"" << path[0] <<"\">" << endl;
			out_.inc();

			// Do not throw exception. Process the next entry inside the current node.
			try {
				block_manager<>::read_ref rr = bm_->read_lock(tree_root);
				node_ref<uint64_traits> n = btree_detail::to_node<uint64_traits>(rr);
				node_info ni;
				convert_to_node_info(n, ni);
				output_node_info(out_, ni);
			} catch (std::exception &e) {
				cerr << e.what() << endl;
			}

			out_.dec();
			out_.indent();
			out_ << "</device>" << endl;
		}
	private:
		block_manager<>::ptr bm_;
		indented_stream& out_;
	};

	//-------------------------------------------------------------------

	struct flags {
		flags() : use_metadata_snap_(false) {
		}

		bool use_metadata_snap_;
		boost::optional<block_address> metadata_snap_;
		boost::optional<block_address> data_mapping_root_;
		boost::optional<block_address> device_details_root_;
		boost::optional<block_address> scan_begin_;
		boost::optional<block_address> scan_end_;
	};

	int low_level_dump_(string const &input,
			    std::ostream &output,
			    flags const &f) {
		block_manager<>::ptr bm = open_bm(input, block_manager<>::READ_ONLY);

		block_address scan_begin = f.scan_begin_ ? *f.scan_begin_ : 0;
		block_address scan_end = f.scan_end_ ? *f.scan_end_ : bm->get_nr_blocks();

		// Allow to read superblock at arbitrary location for low-level dump,
		// without checking equality between the given metadata_snap and sb.metadata_snap_
		superblock_detail::superblock sb = read_superblock(bm, superblock_detail::SUPERBLOCK_LOCATION);
		if (f.use_metadata_snap_) {
			sb = f.metadata_snap_ ?
			     read_superblock(bm, *f.metadata_snap_) :
			     read_superblock(bm, sb.metadata_snap_);
		}
		// override sb.data_mapping_root_
		if (f.data_mapping_root_)
			sb.data_mapping_root_ = *f.data_mapping_root_;
		// override sb.device_details_root_
		if (f.device_details_root_)
			sb.device_details_root_ = *f.device_details_root_;

		transaction_manager::ptr tm = open_tm(bm);

		indented_stream out(output);

		out.indent();
		out << "<superblock blocknr=\"" << sb.blocknr_
		    << "\" data_mapping_root=\"" << sb.data_mapping_root_
		    << "\" device_details_root=\"" << sb.device_details_root_
		    << "\">" << endl;
		out.inc();

		// output the top-level data mapping tree
		ll_mapping_tree_emitter ll_mte(tm->get_bm(), out);
		dev_tree dtree(*tm, sb.data_mapping_root_,
			       mapping_tree_detail::mtree_traits::ref_counter(tm));
		noop_damage_visitor noop_dv;
		btree_visit_values(dtree, ll_mte, noop_dv);

		out.dec();
		out.indent();
		out << "</superblock>" << endl;

		// find orphans
		binary_block_counter bc;
		bc.inc(superblock_detail::SUPERBLOCK_LOCATION);
		count_metadata(tm, sb, bc, true);
		btree_node_checker::ptr filter = btree_node_checker::ptr(
				new unvisited_btree_node_filter(bc));
		base::run_set<block_address> orphans;
		find_btree_nodes(bm, scan_begin, scan_end, filter, orphans);

		// sort orphans
		std::vector<node_info> nodes;
		for (base::run_set<block_address>::const_iterator it = orphans.begin();
		     it != orphans.end();
		     ++it) {
			if (it->begin_ && it->end_) {
				for (block_address b = *it->begin_; b < *it->end_; ++b) {
					block_manager<>::read_ref rr = bm->read_lock(b);
					node_ref<uint64_traits> n = btree_detail::to_node<uint64_traits>(rr);
					nodes.push_back(node_info());
					convert_to_node_info(n, nodes.back());
				}
			}
		}
		std::sort(nodes.begin(), nodes.end(), first_key_cmp);

		// output orphans
		out.indent();
		out << "<orphans>" << std::endl;
		out.inc();

		for (size_t i = 0; i < nodes.size(); ++i)
			output_node_info(out, nodes[i]);

		out.dec();
		out.indent();
		out << "</orphans>" << std::endl;

		return 0;
	}

	int low_level_dump(string const &input,
			   boost::optional<string> output,
			   flags const &f) {
		try {
			if (output) {
				ofstream out(output->c_str());
				low_level_dump_(input, out, f);
			} else
				low_level_dump_(input, cout, f);
		} catch (std::exception &e) {
			cerr << e.what() << endl;
			return 1;
		}
		return 0;
	}
}

//---------------------------------------------------------------------------

thin_ll_dump_cmd::thin_ll_dump_cmd()
	: command("thin_ll_dump")
{
}

void
thin_ll_dump_cmd::usage(ostream &out) const {
	out << "Usage: " << get_name() << " [options] {device|file}" << endl
	    << "Options:" << endl
	    << "  {-h|--help}" << endl
	    << "  {-m|--metadata-snap}[block#]" << endl
	    << "  {-o|--output} <xml file>" << endl
	    << "  {--begin} <block#>" << endl
	    << "  {--end} <block#>" << endl
	    << "  {--data-mapping-root} <block#>" << endl
	    << "  {--device-details-root} <block#>" << endl
	    << "  {-V|--version}" << endl;
}

int
thin_ll_dump_cmd::run(int argc, char **argv)
{
	const char shortopts[] = "hm::o:V";
	const struct option longopts[] = {
		{ "help", no_argument, NULL, 'h'},
		{ "metadata-snap", optional_argument, NULL, 'm'},
		{ "output", required_argument, NULL, 'o'},
		{ "version", no_argument, NULL, 'V'},
		{ "begin", required_argument, NULL, 1},
		{ "end", required_argument, NULL, 2},
		{ "data-mapping-root", required_argument, NULL, 3},
		{ "device-details-root", required_argument, NULL, 4},
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

		case 'm':
			f.use_metadata_snap_ = true;
			if (optarg) {
				try {
					f.metadata_snap_ = boost::lexical_cast<uint64_t>(optarg);
				} catch (std::exception &e) {
					cerr << e.what() << endl;
					return 1;
				}
			}
			break;

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

		case 3:
			try {
				f.data_mapping_root_ = boost::lexical_cast<uint64_t>(optarg);
			} catch (std::exception &e) {
				cerr << e.what() << endl;
				return 1;
			}
			break;

		case 4:
			try {
				f.device_details_root_ = boost::lexical_cast<uint64_t>(optarg);
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
		usage(cerr);
		return 1;
	}

	return low_level_dump(argv[optind], output, f);
}

//---------------------------------------------------------------------------

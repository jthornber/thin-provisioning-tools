#include <iostream>
#include <getopt.h>
#include <libgen.h>
#include <sstream>
#include <vector>

#include "version.h"

#include "persistent-data/data-structures/btree_damage_visitor.h"
#include "persistent-data/range.h"
#include "persistent-data/space-maps/core.h"
#include "thin-provisioning/file_utils.h"
#include "thin-provisioning/superblock.h"
#include "thin-provisioning/mapping_tree.h"

using namespace std;
using namespace thin_provisioning;

//----------------------------------------------------------------

namespace {
	block_manager<>::ptr
	open_bm(string const &path) {
		block_address nr_blocks = get_nr_blocks(path);
		typename block_io<>::mode m = block_io<>::READ_ONLY;
		return block_manager<>::ptr(new block_manager<>(path, nr_blocks, 1, m));
	}

	transaction_manager::ptr
	open_tm(block_manager<>::ptr bm) {
		space_map::ptr sm(new core_map(bm->get_nr_blocks()));
		sm->inc(superblock_detail::SUPERBLOCK_LOCATION);
		transaction_manager::ptr tm(new transaction_manager(bm, sm));
		return tm;
	}

	//--------------------------------

	typedef range<block_address> region;

	using namespace mapping_tree_detail;

	class region_visitor {
	public:
		region_visitor(vector<region> const &regions)
			: regions_(regions) {
		}

		virtual void visit(btree_path const &path, block_time const &bt) {
			if (in_regions(bt.block_)) {
				uint32_t thin_dev = path[0];
				block_address thin_block = path[1];

				cout << "[" << thin_dev
				     << ", " << thin_block
				     << "] -> " << bt.block_
				     << endl;
			}
		}

	private:
		// Slow, but I suspect we wont run with many regions.
		bool in_regions(block_address b) const {
			for (region const &r : regions_)
				if (r.contains(b))
					return true;

			return false;
		}

		vector<region> const &regions_;
	};

	class damage_visitor {
	public:
		virtual void visit(btree_path const &path, btree_detail::damage const &d) {
			throw std::runtime_error("damage in mapping tree, please run thin_check");
		}
	};

	int rmap(string const &path, vector<region> const &regions) {
		block_counter counter; // FIXME: get rid of this counter arg
		region_visitor rv(regions);
		damage_visitor dv;

		try {
			block_manager<>::ptr bm = open_bm(path);
			transaction_manager::ptr tm = open_tm(bm);

			superblock_detail::superblock sb = read_superblock(bm);
			mapping_tree mtree(tm, sb.data_mapping_root_,
					   mapping_tree_detail::block_traits::ref_counter(tm->get_sm()));

			btree_visit_values(mtree, counter, rv, dv);

		} catch (std::exception const &e) {
			cerr << e.what();
			return 1;
		}

		return 0;
	}

	region parse_region(string const &str) {
		istringstream in(str);

		char dots[2] = {'\0', '\0'};
		block_address begin, end;

		in >> begin;
		in.read(dots, sizeof(dots));
		if (dots[0] != '.' || dots[1] != '.')
			throw runtime_error("badly formed region (no dots)");
		in >> end;

		if (in.fail())
			throw runtime_error("badly formed region (couldn't parse numbers)");

		if (end <= begin)
			throw runtime_error("badly formed region (end <= begin)");

		return region(begin, end);
	};

	void usage(ostream &out, string const &cmd) {
		out << "Usage: " << cmd << " [options] {device|file}" << endl
		    << "Options:" << endl
		    << "  {-h|--help}" << endl
		    << "  {-V|--version}" << endl
		    << "  {--region <block range>}*" << endl
		    << "Where:" << endl
		    << "  <block range> is of the form <begin>..<one-past-the-end>" << endl
		    << "  for example 5..45 denotes blocks 5 to 44 inclusive, but not block 45" << endl;
	}
}

//----------------------------------------------------------------

int main(int argc, char **argv)
{
	int c;
	vector<region> regions;
	char const shortopts[] = "hV";
	option const longopts[] = {
		{ "help", no_argument, NULL, 'h'},
		{ "version", no_argument, NULL, 'V'},
		{ "region", required_argument, NULL, 1},
		{ NULL, no_argument, NULL, 0 }
	};

	while ((c = getopt_long(argc, argv, shortopts, longopts, NULL)) != -1) {
		switch (c) {
		case 'h':
			usage(cout, basename(argv[0]));
			return 0;

		case 'V':
			cout << THIN_PROVISIONING_TOOLS_VERSION << endl;
			return 0;

		case 1:
			// region
			regions.push_back(parse_region(optarg));
			break;

		default:
			usage(cerr, basename(argv[0]));
			return 1;
		}
	}

	if (argc == optind) {
		cerr << "No input file provided." << endl;
		usage(cerr, basename(argv[0]));
		exit(1);
	}

	return rmap(argv[optind], regions);
}

//----------------------------------------------------------------

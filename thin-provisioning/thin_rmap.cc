#include <iostream>
#include <getopt.h>
#include <libgen.h>
#include <sstream>
#include <vector>

#include "version.h"

#include "persistent-data/data-structures/btree_damage_visitor.h"
#include "persistent-data/run.h"
#include "persistent-data/space-maps/core.h"
#include "persistent-data/file_utils.h"
#include "thin-provisioning/commands.h"
#include "thin-provisioning/superblock.h"
#include "thin-provisioning/mapping_tree.h"
#include "thin-provisioning/rmap_visitor.h"

using namespace std;
using namespace thin_provisioning;

//----------------------------------------------------------------

namespace {
	block_manager<>::ptr
	open_bm(string const &path) {
		block_address nr_blocks = get_nr_metadata_blocks(path);
		block_manager<>::mode m = block_manager<>::READ_ONLY;
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

	using namespace mapping_tree_detail;

	typedef rmap_visitor::region region;
	typedef rmap_visitor::rmap_region rmap_region;

	class damage_visitor {
	public:
		virtual void visit(btree_path const &path, btree_detail::damage const &d) {
			throw std::runtime_error("Damage in mapping tree, please run thin_check.\n");
		}
	};

	void display_rmap(ostream &out, vector<rmap_region> const &rmap) {
		vector<rmap_region>::const_iterator it;
		for (it = rmap.begin(); it != rmap.end(); ++it) {
			rmap_region const &r = *it;
			out << "data " << r.data_begin
			    << ".." << r.data_end
			    << " -> thin(" << r.thin_dev
			    << ") " << r.thin_begin
			    << ".." << (r.thin_begin + (r.data_end - r.data_begin))
			    << endl;
		}
	}

	int rmap(string const &path, vector<region> const &regions) {
		damage_visitor dv;
		rmap_visitor rv;

		try {
			vector<region>::const_iterator it;
			for (it = regions.begin(); it != regions.end(); ++it)
				rv.add_data_region(*it);

			block_manager<>::ptr bm = open_bm(path);
			transaction_manager::ptr tm = open_tm(bm);

			superblock_detail::superblock sb = read_superblock(bm);
			mapping_tree mtree(*tm, sb.data_mapping_root_,
					   mapping_tree_detail::block_traits::ref_counter(tm->get_sm()));

			btree_visit_values(mtree, rv, dv);
			rv.complete();
			display_rmap(cout, rv.get_rmap());

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
}

//----------------------------------------------------------------

thin_rmap_cmd::thin_rmap_cmd()
	: command("thin_rmap")
{
}

void
thin_rmap_cmd::usage(std::ostream &out) const
{
	out << "Usage: " << get_name() << " [options] {device|file}" << endl
	    << "Options:" << endl
	    << "  {-h|--help}" << endl
	    << "  {-V|--version}" << endl
	    << "  {--region <block range>}*" << endl
	    << "Where:" << endl
	    << "  <block range> is of the form <begin>..<one-past-the-end>" << endl
	    << "  for example 5..45 denotes blocks 5 to 44 inclusive, but not block 45" << endl;
}

int
thin_rmap_cmd::run(int argc, char **argv)
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
			usage(cout);
			return 0;

		case 'V':
			cout << THIN_PROVISIONING_TOOLS_VERSION << endl;
			return 0;

		case 1:
			// region
			try {
				regions.push_back(parse_region(optarg));

			} catch (std::exception const &e) {
				cerr << e.what();
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
		exit(1);
	}

	if (!regions.size()) {
		cerr << "No regions provided." << endl;
		usage(cerr);
		exit(1);
	}

	return rmap(argv[optind], regions);
}

//----------------------------------------------------------------

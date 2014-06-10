#include <boost/lexical_cast.hpp>
#include <boost/optional.hpp>
#include <getopt.h>
#include <iostream>
#include <libgen.h>

#include "version.h"

#include "persistent-data/data-structures/btree_damage_visitor.h"
#include "persistent-data/run.h"
#include "persistent-data/space-maps/core.h"
#include "persistent-data/file_utils.h"
#include "thin-provisioning/superblock.h"
#include "thin-provisioning/mapping_tree.h"

using namespace std;
using namespace thin_provisioning;

//----------------------------------------------------------------

namespace {
	class application {
	public:
		application(string const &cmd)
		: cmd_(cmd) {
		}

		void usage(ostream &out) {
			out << "Usage: " << cmd_ << " [options] --snap1 <snap> --snap2 <snap> <device or file>" << endl
			    << "Options:" << endl
			    << "  {-h|--help}" << endl
			    << "  {-V|--version}" << endl;
		}

		void die(string const &msg) {
			cerr << msg << endl;
			usage(cerr);
			exit(1);
		}

		uint64_t parse_snap(string const &str) {
			try {
				return boost::lexical_cast<uint64_t>(str);

			} catch (...) {
				ostringstream out;
				out << "Couldn't parse snapshot designator: '" << str << "'";
				die(out.str());
			}

			return 0; // never get here
		}

	private:
		string cmd_;
	};

	struct flags {
		boost::optional<string> dev;
		boost::optional<uint64_t> snap1;
		boost::optional<uint64_t> snap2;
	};

	//--------------------------------

	block_manager<>::ptr
	open_bm(string const &path) {
		block_address nr_blocks = get_nr_blocks(path);
		block_io<>::mode m = block_io<>::READ_ONLY;
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

	class delta_visitor {
	public:
		delta_visitor(single_mapping_tree const &origin)
		: origin_(origin) {
			reset_range();
		}

		// This is slow, but easy to write.  Faster would be to
		// iterate both trees simultaneously.
		void visit(btree_path const &path, mapping_tree_detail::block_time const &bt) {
			single_mapping_tree::key k = {path[0]};
			boost::optional<mapping_tree_detail::block_time> origin_bt = origin_.lookup(k);
			if (!origin_bt || origin_bt->block_ != bt.block_)
				emit(path[0], bt.block_);
		}

	private:
		void reset_range() {
			obegin_ = oend_ = 0;
			dbegin_ = dend_ = 0;
		}

		void emit(uint64_t oblock, uint64_t dblock) {
			if (obegin_ == oend_) {
				// We're starting a new range
				obegin_ = oblock;
				oend_ = obegin_;

				dbegin_ = dblock;
				dend_ = dbegin_;

			} else {
				if (oblock != oend_ || dblock != dend_) {
					// Emit the current range ...
					if (oend_ - obegin_ > 1) {
						cout << "<range_mapping origin_begin=\"" << obegin_ << "\""
						     << " data_begin=\"" << dbegin_ << "\""
						     << " length=\"" << oend_ - obegin_ << "\"/>" << endl;
					} else {
						cout << "<single_mapping origin_block=\"" << obegin_ << "\""
						     << " data_block=\"" << dbegin_ << "\"/>" << endl;
					}

					obegin_ = oblock;
					oend_ = obegin_;

					dbegin_ = dblock;
					dend_ = dbegin_;
				}
			}

			oend_++;
			dend_++;
		}

		uint64_t obegin_, oend_;
		uint64_t dbegin_, dend_;

		single_mapping_tree const &origin_;
	};

	class damage_visitor {
	public:
		virtual void visit(btree_path const &path, btree_detail::damage const &d) {
			throw std::runtime_error("damage in mapping tree, please run thin_check");
		}
	};

	void delta_(application &app, flags const &fs) {
		block_manager<>::ptr bm = open_bm(*fs.dev);
		transaction_manager::ptr tm = open_tm(bm);

		superblock_detail::superblock sb = read_superblock(bm);

		dev_tree dtree(tm, sb.data_mapping_root_,
			       mapping_tree_detail::mtree_traits::ref_counter(tm));

		dev_tree::key k = {*fs.snap1};
		boost::optional<uint64_t> snap1_root = dtree.lookup(k);

		if (!snap1_root) {
			ostringstream out;
			out << "Unable to find mapping tree for snap1 (" << *fs.snap1 << ")";
			app.die(out.str());
		}

		single_mapping_tree snap1(tm, *snap1_root, mapping_tree_detail::block_traits::ref_counter(tm->get_sm()));

		k[0] = *fs.snap2;
		boost::optional<uint64_t> snap2_root = dtree.lookup(k);

		if (!snap2_root) {
			ostringstream out;
			out << "Unable to find mapping tree for snap2 (" << *fs.snap2 << ")";
			app.die(out.str());
		}

		single_mapping_tree snap2(tm, *snap2_root, mapping_tree_detail::block_traits::ref_counter(tm->get_sm()));

		delta_visitor delta_v(snap1);
		damage_visitor damage_v;
		btree_visit_values(snap2, delta_v, damage_v);
	}

	int delta(application &app, flags const &fs) {
		try {
			delta_(app, fs);
		} catch (exception const &e) {
			app.die(e.what());
			return 1; // never get here
		}

		return 0;
	}
}

//----------------------------------------------------------------

// FIXME: add metadata snap switch

int main(int argc, char **argv)
{
	int c;
	flags fs;
	application app(basename(argv[0]));

	char const shortopts[] = "hV";
	option const longopts[] = {
		{ "help", no_argument, NULL, 'h' },
		{ "version", no_argument, NULL, 'V' },
		{ "snap1", required_argument, NULL, 1 },
		{ "snap2", required_argument, NULL, 2 },
		{ "metadata-snap", optional_argument, NULL, 3 },
	};

	while ((c = getopt_long(argc, argv, shortopts, longopts, NULL)) != -1) {
		switch (c) {
		case 'h':
			app.usage(cout);
			return 0;

		case 'V':
			cout << THIN_PROVISIONING_TOOLS_VERSION << endl;
			return 0;

		case 1:
			fs.snap1 = app.parse_snap(optarg);
			break;

		case 2:
			fs.snap2 = app.parse_snap(optarg);
			break;

		default:
			app.usage(cerr);
			return 1;
		}
	}

	if (argc == optind)
		app.die("No input device provided.");
	else
		fs.dev = argv[optind];

	if (!fs.snap1)
		app.die("--snap1 not specified.");

	if (!fs.snap2)
		app.die("--snap2 not specified.");

	return delta(app, fs);
}

//----------------------------------------------------------------

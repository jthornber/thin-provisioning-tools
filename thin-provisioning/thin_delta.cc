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

	struct mapping {
		mapping()
			: vbegin_(0),
			  dbegin_(0),
			  len_(0) {
		}

		mapping(uint64_t vbegin, uint64_t dbegin, uint64_t len)
			: vbegin_(vbegin),
			  dbegin_(dbegin),
			  len_(len) {
		}

		void consume(uint64_t delta) {
			delta = min<uint64_t>(delta, len_);
			vbegin_ += delta;
			dbegin_ += delta;
			len_ -= delta;
		}

		uint64_t vbegin_, dbegin_, len_;
	};

	typedef std::deque<mapping> mapping_deque;

	// Builds up an in core rep of the mappings for a device.
	class mapping_recorder {
	public:
		mapping_recorder() {
			reset_range();
		}

		void visit(btree_path const &path, mapping_tree_detail::block_time const &bt) {
			record(path[0], bt.block_);
		}

		mapping_deque const &get_mappings() const {
			return mappings_;
		}

	private:
		void reset_range() {
			obegin_ = oend_ = 0;
			dbegin_ = dend_ = 0;
		}

		void record(uint64_t oblock, uint64_t dblock) {
			if (obegin_ == oend_) {
				// We're starting a new range
				obegin_ = oblock;
				oend_ = obegin_;

				dbegin_ = dblock;
				dend_ = dbegin_;

			} else {
				if (oblock != oend_ || dblock != dend_) {
					// Emit the current range ...
					push_mapping(obegin_, dbegin_, oend_ - obegin_);

					obegin_ = oblock;
					oend_ = obegin_;

					dbegin_ = dblock;
					dend_ = dbegin_;
				}
			}

			oend_++;
			dend_++;
		}

		void push_mapping(uint64_t vbegin, uint64_t dbegin, uint64_t len) {
			mappings_.push_back(mapping(vbegin, dbegin, len));
		}

		uint64_t obegin_, oend_;
		uint64_t dbegin_, dend_;

		mapping_deque mappings_;
	};

	//--------------------------------

	class damage_visitor {
	public:
		virtual void visit(btree_path const &path, btree_detail::damage const &d) {
			throw std::runtime_error("damage in mapping tree, please run thin_check");
		}
	};

	class diff_emitter {
	public:
		diff_emitter(ostream &out)
		: out_(out) {
		}

		void left_only(uint64_t vbegin, uint64_t dbegin, uint64_t len) {
			begin_block(LEFT_ONLY);
			out_ << "  <rang begin=\"" << vbegin << "\""
			     << " data_begin=\"" << dbegin << "\""
			     << " length=\"" << len << "\"/>\n";
		}

		void right_only(uint64_t vbegin, uint64_t dbegin, uint64_t len) {
			begin_block(RIGHT_ONLY);
			out_ << "  <range begin=\"" << vbegin << "\""
			     << " data_begin=\"" << dbegin << "\""
			     << " length=\"" << len << "\"/>\n";
		}

		void blocks_differ(uint64_t vbegin, uint64_t left_dbegin, uint64_t right_dbegin, uint64_t len) {
			begin_block(DIFFER);
			out_ << "  <range begin=\"" << vbegin << "\""
			     << " left_data_begin=\"" << left_dbegin << "\""
			     << " right_data_begin=\"" << right_dbegin << "\""
			     << " length=\"" << len << "\"/>\n";
		}

		void blocks_same(uint64_t vbegin, uint64_t dbegin, uint64_t len) {
			begin_block(SAME);
			out_ << "  <range begin=\"" << vbegin << "\""
			     << " data_begin=\"" << dbegin << "\""
			     << " length=\"" << len << "\"/>\n";
		}

		void complete() {
			if (current_type_)
				close(*current_type_);
		}

	private:
		enum block_type {
			LEFT_ONLY,
			RIGHT_ONLY,
			DIFFER,
			SAME
		};

		void begin_block(block_type t) {
			if (!current_type_) {
				current_type_ = t;
				open(t);

			} else if (*current_type_ != t) {
				close(*current_type_);
				current_type_ = t;
				open(t);
			}
		}

		void open(block_type t) {
			switch (t) {
			case LEFT_ONLY:
				out_ << "<left_only>\n";
				break;

			case RIGHT_ONLY:
				out_ << "<right_only>\n";
				break;

			case DIFFER:
				out_ << "<different>\n";
				break;

			case SAME:
				out_ << "<same>\n";
				break;
			}
		}

		void close(block_type t) {
			switch (t) {
			case LEFT_ONLY:
				out_ << "</left_only>\n\n";
				break;

			case RIGHT_ONLY:
				out_ << "</right_only>\n\n";
				break;

			case DIFFER:
				out_ << "</different>\n\n";
				break;

			case SAME:
				out_ << "</same>\n\n";
				break;
			}

		}

		boost::optional<block_type> current_type_;
		ostream &out_;
	};

	//----------------------------------------------------------------

	void dump_diff(mapping_deque const &left,
		       mapping_deque const &right) {

		diff_emitter e(cout);

		// We iterate through both sets of mappings in parallel
		// noting any differences.
		mapping_deque::const_iterator left_it = left.begin();
		mapping_deque::const_iterator right_it = right.begin();

		mapping left_mapping;
		mapping right_mapping;

		while (left_it != left.end() && right_it != right.end()) {
			if (!left_mapping.len_ && left_it != left.end())
				left_mapping = *left_it++;

			if (!right_mapping.len_ && right_it != right.end())
				right_mapping = *right_it++;

			while (left_mapping.len_ && right_mapping.len_) {
				if (left_mapping.vbegin_ < right_mapping.vbegin_) {
					uint64_t delta = min<uint64_t>(left_mapping.len_, right_mapping.vbegin_ - left_mapping.vbegin_);
					e.left_only(left_mapping.vbegin_, left_mapping.dbegin_, delta);
					left_mapping.consume(delta);

				} else if (left_mapping.vbegin_ > left_mapping.vbegin_) {
					uint64_t delta = min<uint64_t>(right_mapping.len_, left_mapping.vbegin_ - right_mapping.vbegin_);
					e.right_only(right_mapping.vbegin_, right_mapping.dbegin_, delta);
					right_mapping.consume(delta);

				} else if (left_mapping.dbegin_ != right_mapping.dbegin_) {
					uint64_t delta = min<uint64_t>(left_mapping.len_, right_mapping.len_);
					e.blocks_differ(left_mapping.vbegin_, left_mapping.dbegin_, right_mapping.dbegin_, delta);
					left_mapping.consume(delta);
					right_mapping.consume(delta);

				} else {
					uint64_t delta = min<uint64_t>(left_mapping.len_, right_mapping.len_);
					e.blocks_same(left_mapping.vbegin_, left_mapping.dbegin_, delta);
					left_mapping.consume(delta);
					right_mapping.consume(delta);
				}
			}
		}

		e.complete();
	}

	void delta_(application &app, flags const &fs) {
		mapping_recorder mr1;
		mapping_recorder mr2;
		damage_visitor damage_v;

		{
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
			btree_visit_values(snap1, mr1, damage_v);
			btree_visit_values(snap2, mr2, damage_v);
		}

		dump_diff(mr1.get_mappings(), mr2.get_mappings());
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

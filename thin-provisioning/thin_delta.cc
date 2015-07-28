#include <boost/lexical_cast.hpp>
#include <boost/optional.hpp>
#include <getopt.h>
#include <iostream>
#include <libgen.h>

#include "version.h"

#include "base/indented_stream.h"
#include "persistent-data/data-structures/btree_damage_visitor.h"
#include "persistent-data/run.h"
#include "persistent-data/space-maps/core.h"
#include "persistent-data/space-maps/disk.h"
#include "persistent-data/file_utils.h"
#include "thin-provisioning/superblock.h"
#include "thin-provisioning/mapping_tree.h"
#include "thin-provisioning/commands.h"

using namespace std;
using namespace thin_provisioning;

//----------------------------------------------------------------

namespace local {
	class application {
	public:
		application(string const &cmd)
		: cmd_(cmd) {
		}

		void usage(ostream &out) {
			out << "Usage: " << cmd_ << " [options] <device or file>\n"
			    << "Options:\n"
			    << "  {--thin1, --snap1}\n"
			    << "  {--thin2, --snap2}\n"
			    << "  {-m, --metadata-snap} [block#]\n"
			    << "  {--verbose}\n"
			    << "  {-h|--help}\n"
			    << "  {-V|--version}" << endl;
		}

		void die(string const &msg) {
			cerr << msg << endl;
			usage(cerr);
			exit(1);
		}

		uint64_t parse_int(string const &str, string const &desc) {
			try {
				return boost::lexical_cast<uint64_t>(str);

			} catch (...) {
				ostringstream out;
				out << "Couldn't parse " << desc << ": '" << str << "'";
				die(out.str());
			}

			return 0; // never get here
		}

	private:
		string cmd_;
	};

	struct flags {
		flags()
			: verbose(false),
			  find_metadata_snap(false) {
		}

		boost::optional<string> dev;
		boost::optional<uint64_t> metadata_snap;
		boost::optional<uint64_t> snap1;
		boost::optional<uint64_t> snap2;
		bool verbose;
		bool find_metadata_snap;
	};

	//--------------------------------

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

	ostream &operator <<(ostream &out, mapping const &m) {
		out << "mapping[vbegin = " << m.vbegin_
		    << ", dbegin = " << m.dbegin_
		    << ", len = " << m.len_ << "]";
		return out;
	}

	typedef std::deque<mapping> mapping_deque;

	// Builds up an in core rep of the mappings for a device.
	class mapping_recorder {
	public:
		mapping_recorder() {
			no_range();
		}

		void visit(btree_path const &path, mapping_tree_detail::block_time const &bt) {
			record(path[0], bt.block_);
		}

		void complete() {
			if (range_in_progress()) {
				push_range();
				no_range();
			}
		}

		mapping_deque const &get_mappings() const {
			return mappings_;
		}

	private:
		void no_range() {
			obegin_ = oend_ = 0;
			dbegin_ = dend_ = 0;
		}

		void inc_range() {
			oend_++;
			dend_++;
		}

		void begin_range(uint64_t oblock, uint64_t dblock) {
			obegin_ = oend_ = oblock;
			dbegin_ = dend_ = dblock;
			inc_range();
		}

		bool range_in_progress() {
			return oend_ != obegin_;
		}

		bool continues_range(uint64_t oblock, uint64_t dblock) {
			return (oblock == oend_) && (dblock == dend_);
		}

		void push_range() {
			mapping m(obegin_, dbegin_, oend_ - obegin_);
			mappings_.push_back(m);
		}

		void record(uint64_t oblock, uint64_t dblock) {
			if (!range_in_progress())
				begin_range(oblock, dblock);

			else if (!continues_range(oblock, dblock)) {
				push_range();
				begin_range(oblock, dblock);
			} else
				inc_range();
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

	//--------------------------------

	class diff_emitter {
	public:
		diff_emitter(indented_stream &out)
		: out_(out) {
		}

		virtual void left_only(uint64_t vbegin, uint64_t dbegin, uint64_t len) = 0;
		virtual void right_only(uint64_t vbegin, uint64_t dbegin, uint64_t len) = 0;
		virtual void blocks_differ(uint64_t vbegin, uint64_t left_dbegin, uint64_t right_dbegin, uint64_t len) = 0;
		virtual void blocks_same(uint64_t vbegin, uint64_t dbegin, uint64_t len) = 0;
		virtual void complete() = 0;

	protected:
		void indent() {
			out_.indent();
		}

		indented_stream &out() {
			return out_;
		}

	private:
		indented_stream &out_;
	};


	class simple_emitter : public diff_emitter {
	public:
		simple_emitter(indented_stream &out)
		: diff_emitter(out) {
		}

		void left_only(uint64_t vbegin, uint64_t dbegin, uint64_t len) {
			add_range(LEFT_ONLY, vbegin, len);
		}

		void right_only(uint64_t vbegin, uint64_t dbegin, uint64_t len) {
			add_range(RIGHT_ONLY, vbegin, len);
		}

		void blocks_differ(uint64_t vbegin, uint64_t left_dbegin, uint64_t right_dbegin, uint64_t len) {
			add_range(DIFFER, vbegin, len);
		}

		void blocks_same(uint64_t vbegin, uint64_t dbegin, uint64_t len) {
			add_range(SAME, vbegin, len);
		}

		void complete() {
			if (current_type_)
				emit_range();
		}

	private:
		enum block_type {
			LEFT_ONLY,
			RIGHT_ONLY,
			DIFFER,
			SAME
		};

		void add_range(block_type t, uint64_t vbegin, uint64_t len) {
			if (current_type_ && *current_type_ == t && vbegin == vend_) {
				vend_ += len;
				return;
			}

			emit_range();
			current_type_ = t;
			vbegin_ = vbegin;
			vend_ = vbegin_ + len;
		}

		void emit_range() {
			if (!current_type_)
				return;

			indent();
			switch (*current_type_) {
			case LEFT_ONLY:
				out() << "<left_only";
				break;

			case RIGHT_ONLY:
				out() << "<right_only";
				break;

			case DIFFER:
				out() << "<different";
				break;

			case SAME:
				out() << "<same";
				break;
			}

			out() << " begin=\"" << vbegin_ << "\""
			      << " length=\"" << vend_ - vbegin_ << "\"/>\n";
		}

		boost::optional<block_type> current_type_;
		uint64_t vbegin_, vend_;
	};

	class verbose_emitter : public diff_emitter {
	public:
		verbose_emitter(indented_stream &out)
		: diff_emitter(out) {
		}

		void left_only(uint64_t vbegin, uint64_t dbegin, uint64_t len) {
			begin_block(LEFT_ONLY);
			indent();
			out() << "<range begin=\"" << vbegin << "\""
			      << " data_begin=\"" << dbegin << "\""
			      << " length=\"" << len << "\"/>\n";
		}

		void right_only(uint64_t vbegin, uint64_t dbegin, uint64_t len) {
			begin_block(RIGHT_ONLY);
			indent();
			out() << "<range begin=\"" << vbegin << "\""
			      << " data_begin=\"" << dbegin << "\""
			      << " length=\"" << len << "\"/>\n";
		}

		void blocks_differ(uint64_t vbegin, uint64_t left_dbegin, uint64_t right_dbegin, uint64_t len) {
			begin_block(DIFFER);
			indent();
			out() << "<range begin=\"" << vbegin << "\""
			      << " left_data_begin=\"" << left_dbegin << "\""
			      << " right_data_begin=\"" << right_dbegin << "\""
			      << " length=\"" << len << "\"/>\n";
		}

		void blocks_same(uint64_t vbegin, uint64_t dbegin, uint64_t len) {
			begin_block(SAME);
			indent();
			out() << "<range begin=\"" << vbegin << "\""
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
			indent();
			switch (t) {
			case LEFT_ONLY:
				out() << "<left_only>\n";
				break;

			case RIGHT_ONLY:
				out() << "<right_only>\n";
				break;

			case DIFFER:
				out() << "<different>\n";
				break;

			case SAME:
				out() << "<same>\n";
				break;
			}
			out().inc();
		}

		void close(block_type t) {
			out().dec();
			indent();
			switch (t) {
			case LEFT_ONLY:
				out() << "</left_only>\n";
				break;

			case RIGHT_ONLY:
				out() << "</right_only>\n";
				break;

			case DIFFER:
				out() << "</different>\n";
				break;

			case SAME:
				out() << "</same>\n";
				break;
			}

		}

		boost::optional<block_type> current_type_;
	};

	//----------------------------------------------------------------

	void dump_diff(mapping_deque const &left,
		       mapping_deque const &right,
		       diff_emitter &e) {

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

				} else if (left_mapping.vbegin_ > right_mapping.vbegin_) {
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

		while (left_it != left.end()) {
			left_mapping = *left_it++;

			if (left_mapping.len_)
				e.left_only(left_mapping.vbegin_, left_mapping.dbegin_, left_mapping.len_);
		}

		while (right_it != right.end()) {
			right_mapping = *right_it++;

			if (right_mapping.len_)
				e.right_only(right_mapping.vbegin_, right_mapping.dbegin_, right_mapping.len_);
		}

		e.complete();
	}

	// FIXME: duplication with xml_format
	void begin_superblock(indented_stream &out,
			      string const &uuid,
			      uint64_t time,
			      uint64_t trans_id,
			      uint32_t data_block_size,
			      uint64_t nr_data_blocks,
			      boost::optional<uint64_t> metadata_snap) {
		out.indent();
		out << "<superblock uuid=\"" << uuid << "\""
		    << " time=\"" << time << "\""
		    << " transaction=\"" << trans_id << "\""
		    << " data_block_size=\"" << data_block_size << "\""
		    << " nr_data_blocks=\"" << nr_data_blocks;

		if (metadata_snap)
			out << "\" metadata_snap=\"" << *metadata_snap;

		out << "\">\n";
		out.inc();
	}

	void end_superblock(indented_stream &out) {
		out.dec();
		out.indent();
		out << "</superblock>\n";
	}

	void begin_diff(indented_stream &out, uint64_t snap1, uint64_t snap2) {
		out.indent();
		out << "<diff left=\"" << snap1 << "\" right=\"" << snap2 << "\">\n";
		out.inc();
	}

	void end_diff(indented_stream &out) {
		out.dec();
		out.indent();
		out << "</diff>\n";
	}

	void delta_(application &app, flags const &fs) {
		mapping_recorder mr1;
		mapping_recorder mr2;
		damage_visitor damage_v;
		superblock_detail::superblock sb;
		checked_space_map::ptr data_sm;

		{
			block_manager<>::ptr bm = open_bm(*fs.dev, block_manager<>::READ_ONLY, !fs.metadata_snap);
			transaction_manager::ptr tm = open_tm(bm);

			sb = fs.metadata_snap ? read_superblock(bm, *fs.metadata_snap) : read_superblock(bm);
			data_sm = open_disk_sm(*tm, static_cast<void *>(&sb.data_space_map_root_));

			dev_tree dtree(*tm, sb.data_mapping_root_,
				       mapping_tree_detail::mtree_traits::ref_counter(tm));

			dev_tree::key k = {*fs.snap1};
			boost::optional<uint64_t> snap1_root = dtree.lookup(k);

			if (!snap1_root) {
				ostringstream out;
				out << "Unable to find mapping tree for snap1 (" << *fs.snap1 << ")";
				app.die(out.str());
			}

			single_mapping_tree snap1(*tm, *snap1_root, mapping_tree_detail::block_traits::ref_counter(tm->get_sm()));

			k[0] = *fs.snap2;
			boost::optional<uint64_t> snap2_root = dtree.lookup(k);

			if (!snap2_root) {
				ostringstream out;
				out << "Unable to find mapping tree for snap2 (" << *fs.snap2 << ")";
				app.die(out.str());
			}

			single_mapping_tree snap2(*tm, *snap2_root, mapping_tree_detail::block_traits::ref_counter(tm->get_sm()));
			btree_visit_values(snap1, mr1, damage_v);
			mr1.complete();

			btree_visit_values(snap2, mr2, damage_v);
			mr2.complete();
		}

		indented_stream is(cout);
		begin_superblock(is, "", sb.time_,
				 sb.trans_id_,
				 sb.data_block_size_,
				 data_sm->get_nr_blocks(),
				 sb.metadata_snap_ ?
				 boost::optional<block_address>(sb.metadata_snap_) :
				 boost::optional<block_address>());
		begin_diff(is, *fs.snap1, *fs.snap2);

		if (fs.verbose) {
			verbose_emitter e(is);
			dump_diff(mr1.get_mappings(), mr2.get_mappings(), e);
		} else {
			simple_emitter e(is);
			dump_diff(mr1.get_mappings(), mr2.get_mappings(), e);
		}

		end_diff(is);
		end_superblock(is);
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

int thin_delta_main(int argc, char **argv)
{
	using namespace local;

	int c;
	flags fs;
	local::application app(basename(argv[0]));

	char const shortopts[] = "hVm::";
	option const longopts[] = {
		{ "help", no_argument, NULL, 'h' },
		{ "version", no_argument, NULL, 'V' },
		{ "thin1", required_argument, NULL, 1 },
		{ "snap1", required_argument, NULL, 1 },
		{ "thin2", required_argument, NULL, 2 },
		{ "snap2", required_argument, NULL, 2 },
		{ "metadata-snap", optional_argument, NULL, 'm' },
		{ "verbose", no_argument, NULL, 4 }
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
			fs.snap1 = app.parse_int(optarg, "thin id 1");
			break;

		case 2:
			fs.snap2 = app.parse_int(optarg, "thin id 2");
			break;

		case 'm':
			if (optarg)
				fs.metadata_snap = app.parse_int(optarg, "metadata snapshot block");
			else
				fs.find_metadata_snap = true;
			break;

		case 4:
			fs.verbose = true;
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

	if (fs.find_metadata_snap) {
		fs.metadata_snap = find_metadata_snap(*fs.dev);
		cerr << "metadata snap = " << fs.metadata_snap << "\n";
	}

	return delta(app, fs);
}

base::command thin_provisioning::thin_delta_cmd("thin_delta", thin_delta_main);

//----------------------------------------------------------------

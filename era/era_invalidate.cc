#include <fstream>
#include <getopt.h>
#include <libgen.h>
#include <iostream>

#include "version.h"
#include "base/indented_stream.h"
#include "era/commands.h"
#include "era/era_array.h"
#include "era/writeset_tree.h"
#include "era/metadata.h"
#include "era/xml_format.h"
#include "persistent-data/file_utils.h"

#include <boost/lexical_cast.hpp>

using namespace boost;
using namespace era;
using namespace std;

//----------------------------------------------------------------

namespace {
	struct flags {
		flags()
			: metadata_snapshot_(false) {
		}

		bool metadata_snapshot_;
		optional<uint32_t> era_threshold_;
	};

	//--------------------------------

	void walk_array(era_array const &array, uint32_t nr_blocks,
			uint32_t threshold, set<uint32_t> &blocks) {
		for (uint32_t b = 0; b < nr_blocks; b++) {
			uint32_t era = array.get(b);
			if (era >= threshold)
				blocks.insert(b);
		}
	}

	class writesets_marked_since : public writeset_tree_detail::writeset_visitor {
	public:
		writesets_marked_since(uint32_t threshold, set<uint32_t> &blocks)
			: current_era_(0),
			  threshold_(threshold),
			  blocks_(blocks) {
		}

		void writeset_begin(uint32_t era, uint32_t nr_bits) {
			current_era_ = era;
		}

		void bit(uint32_t index, bool value) {
			if (value && current_era_ >= threshold_)
				blocks_.insert(index);
		}

		void writeset_end() {
		}

	private:
		uint32_t current_era_;
		uint32_t threshold_;
		set<uint32_t> &blocks_;
	};

	void raise_metadata_damage() {
		throw std::runtime_error("metadata contains errors (run era_check for details).");
	}

	struct fatal_writeset_tree_damage : public writeset_tree_detail::damage_visitor {
		void visit(writeset_tree_detail::missing_eras const &d) {
			raise_metadata_damage();
		}

		void visit(writeset_tree_detail::damaged_writeset const &d) {
			raise_metadata_damage();
		}
	};

	void walk_writesets(metadata const &md, uint32_t threshold, set<uint32_t> &result) {
		writesets_marked_since v(threshold, result);
		fatal_writeset_tree_damage dv;

		walk_writeset_tree(md.tm_, *md.writeset_tree_, v, dv);
	}

	void mark_blocks_since(metadata const &md, optional<uint32_t> const &threshold, set<uint32_t> &result) {
		if (!threshold)
			// Can't get here, just putting in to pacify the compiler
			throw std::runtime_error("threshold not set");
		else {
			walk_array(*md.era_array_, md.sb_.nr_blocks, *threshold, result);
			walk_writesets(md, *threshold, result);
		}
	}

	//--------------------------------

	template <typename Iterator>
	pair<uint32_t, uint32_t> next_run(Iterator &it, Iterator end) {
		uint32_t b, e;

		b = *it++;
		e = b + 1;
		while (it != end && *it == e) {
			e++;
			it++;
		}

		return make_pair(b, e);
	}

	void emit_blocks(ostream &out, set<uint32_t> const &blocks) {
		indented_stream o(out);

		o.indent();
		o << "<blocks>" << endl;

		o.inc();
		{
			set<uint32_t>::const_iterator it = blocks.begin();
			while (it != blocks.end()) {
				o.indent();

				pair<uint32_t, uint32_t> range = next_run(it, blocks.end());
				if (range.second - range.first == 1)
					o << "<block block=\"" << range.first << "\"/>" << endl;

				else
					o << "<range begin=\"" << range.first
					  << "\" end = \"" << range.second << "\"/>" << endl;
			}
		}
		o.dec();

		o.indent();
		o << "</blocks>" << endl;
	}

	//--------------------------------

	string const STDOUT_PATH("-");

	bool want_stdout(string const &output) {
		return output == STDOUT_PATH;
	}

	int invalidate(string const &dev, string const &output, flags const &fs) {
		try {
			set<uint32_t> blocks;
			block_manager<>::ptr bm = open_bm(dev, block_manager<>::READ_ONLY);

			if (fs.metadata_snapshot_) {
				superblock sb = read_superblock(bm);
				if (!sb.metadata_snap)
					throw runtime_error("no metadata snapshot taken.");

				metadata::ptr md(new metadata(bm, *sb.metadata_snap));
				mark_blocks_since(*md, fs.era_threshold_, blocks);

			} else {
				metadata::ptr md(new metadata(bm, metadata::OPEN));
				mark_blocks_since(*md, fs.era_threshold_, blocks);
			}

			if (want_stdout(output))
				emit_blocks(cout, blocks);

			else {
				ofstream out(output.c_str());
				emit_blocks(out, blocks);
			}

		} catch (std::exception &e) {
			cerr << e.what() << endl;
			return 1;
		}

		return 0;
	}

	void usage(ostream &out, string const &cmd) {
		out << "Usage: " << cmd << " [options] --written-since <era> {device|file}\n"
		    << "Options:\n"
		    << "  {-h|--help}\n"
		    << "  {-o <xml file>}\n"
		    << "  {--metadata-snapshot}\n"
		    << "  {-V|--version}" << endl;
	}
}

//----------------------------------------------------------------

int era_invalidate_main(int argc, char **argv)
{
	int c;
	flags fs;
	string output("-");
	char const shortopts[] = "ho:V";

	option const longopts[] = {
		{ "help", no_argument, NULL, 'h' },
		{ "output", required_argument, NULL, 'o' },
		{ "version", no_argument, NULL, 'V' },
		{ "metadata-snapshot", no_argument, NULL, 1},
		{ "written-since", required_argument, NULL, 2},
		{ NULL, no_argument, NULL, 0 }
	};

	while ((c = getopt_long(argc, argv, shortopts, longopts, NULL)) != -1) {
		switch(c) {
		case 1:
			fs.metadata_snapshot_ = true;
			break;

		case 2:
			fs.era_threshold_ = lexical_cast<uint32_t>(optarg);
			break;

		case 'h':
			usage(cout, basename(argv[0]));
			return 0;

		case 'o':
			output = optarg;
			break;

		case 'V':
			cout << THIN_PROVISIONING_TOOLS_VERSION << endl;
			return 0;

		default:
			usage(cerr, basename(argv[0]));
			return 1;
		}
	}

	if (argc == optind) {
		cerr << "No input file provided." << endl;
		usage(cerr, basename(argv[0]));
		return 1;
	}

	if (!fs.era_threshold_) {
		cerr << "Please specify --written-since" << endl;
		usage(cerr, basename(argv[0]));
		return 1;
	}

	return invalidate(argv[optind], output, fs);
}

base::command era::era_invalidate_cmd("era_invalidate", era_invalidate_main);

//----------------------------------------------------------------

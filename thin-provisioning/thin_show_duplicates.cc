// Copyright (C) 2015 Red Hat, Inc. All rights reserved.
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

#include <iostream>
#include <getopt.h>
#include <libgen.h>

#include "version.h"

#include "base/application.h"
#include "base/error_state.h"
#include "base/progress_monitor.h"
#include "persistent-data/data-structures/btree_damage_visitor.h"
#include "persistent-data/file_utils.h"
#include "persistent-data/space-maps/core.h"
#include "persistent-data/space-maps/disk.h"
#include "thin-provisioning/cache_stream.h"
#include "thin-provisioning/pool_stream.h"
#include "thin-provisioning/commands.h"
#include "thin-provisioning/device_tree.h"
#include "thin-provisioning/mapping_tree.h"
#include "thin-provisioning/rmap_visitor.h"
#include "thin-provisioning/superblock.h"

#include <boost/uuid/sha1.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/optional.hpp>
#include <deque>
#include <vector>

using namespace base;
using namespace boost;
using namespace persistent_data;
using namespace std;
using namespace thin_provisioning;

//----------------------------------------------------------------

namespace {
	bool factor_of(block_address f, block_address n) {
		cerr << n << " % " << f << "\n";
		return (n % f) == 0;
	}

	block_manager<>::ptr
	open_bm(string const &path) {
		block_address nr_blocks = get_nr_blocks(path);
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

	uint64_t parse_int(string const &str, string const &desc) {
		try {
			return boost::lexical_cast<uint64_t>(str);

		} catch (...) {
			ostringstream out;
			out << "Couldn't parse " << desc << ": '" << str << "'";
			exit(1);
		}

		return 0; // never get here
	}

	//--------------------------------

	struct flags {
		flags()
			: cache_mem(64 * 1024 * 1024) {
		}

		string data_dev;
		optional<string> metadata_dev;
		optional<unsigned> block_size;
		unsigned cache_mem;
	};

	// FIXME: introduce abstraction for a stream of segments

	using namespace mapping_tree_detail;

	class duplicate_counter {
	public:
		duplicate_counter(block_address nr_blocks)
		: counts_(nr_blocks),
		  total_dups_(0) {
		}

		void add_duplicate(block_address b1, block_address b2) {
			total_dups_++;
			counts_[b1]++;
		}

		block_address get_total() const {
			return total_dups_;
		}

	private:
		vector<block_address> counts_;
		block_address total_dups_;
	};

	class duplicate_detector {
	public:
		duplicate_detector(unsigned block_size, block_address nr_blocks)
			: block_size_(block_size),
			  results_(nr_blocks) {
		}

		// FIXME: remove
		void examine(block_cache::block const &b) {
			digestor_.reset();
			digestor_.process_bytes(b.get_data(), block_size_);
			unsigned int digest[5];
			digestor_.get_digest(digest);

			// hack
			vector<unsigned int> v(5);
			for (unsigned i = 0; i < 5; i++)
				v[i] = digest[i];

			fingerprint_map::const_iterator it = fm_.find(v);
			if (it != fm_.end()) {
				results_.add_duplicate(it->second, b.get_index());
			} else
				fm_.insert(make_pair(v, b.get_index()));
		}

		void examine(chunk const &c) {
			digestor_.reset();

			for (deque<mem>::const_iterator it = c.mem_.begin(); it != c.mem_.end(); it++)
				digestor_.process_bytes(it->begin, it->end - it->begin);

			unsigned int digest[5];
			digestor_.get_digest(digest);

			// hack
			vector<unsigned int> v(5);
			for (unsigned i = 0; i < 5; i++)
				v[i] = digest[i];

			fingerprint_map::const_iterator it = fm_.find(v);
			block_address index = (c.offset_sectors_ * 512) / block_size_;
			if (it != fm_.end()) {
				results_.add_duplicate(it->second, index);
			} else
				fm_.insert(make_pair(v, index));
		}

		block_address get_total_duplicates() const {
			return results_.get_total();
		}

	private:
		typedef map<vector<unsigned int>, block_address> fingerprint_map;

		unsigned block_size_;
		boost::uuids::detail::sha1 digestor_;
		fingerprint_map fm_;
		duplicate_counter results_;
	};

	int show_dups_pool(flags const &fs) {
		block_manager<>::ptr bm = open_bm(*fs.metadata_dev);
		transaction_manager::ptr tm = open_tm(bm);
		superblock_detail::superblock sb = read_superblock(bm);
		block_address block_size = sb.data_block_size_ * 512;
		block_address nr_blocks = get_nr_blocks(fs.data_dev, block_size);

		cerr << "path = " << fs.data_dev << "\n";
		cerr << "block size = " << block_size << "\n";
		cerr << "nr_blocks = " << nr_blocks << "\n";

		cache_stream stream(fs.data_dev, block_size, fs.cache_mem);
		pool_stream pstream(stream, tm, sb, nr_blocks);

		duplicate_detector detector(block_size, nr_blocks);
		auto_ptr<progress_monitor> pbar = create_progress_bar("Examining data");

		do {
			chunk const &c = pstream.get();
			detector.examine(c);
			pbar->update_percent((pstream.index() * 100) / pstream.nr_chunks());

		} while (pstream.advance());
		pbar->update_percent(100);

		cout << "\n\ntotal dups: " << detector.get_total_duplicates() << endl;
		cout << (detector.get_total_duplicates() * 100) / pstream.nr_chunks() << "% duplicates\n";

		return 0;
	}

	int show_dups_linear(flags const &fs) {
		if (!fs.block_size)
			// FIXME: this check should be moved to the switch parsing
			throw runtime_error("--block-sectors or --metadata-dev must be supplied");

		block_address block_size = *fs.block_size;
		block_address nr_blocks = get_nr_blocks(fs.data_dev, *fs.block_size);

		cerr << "path = " << fs.data_dev << "\n";
		cerr << "nr_blocks = " << nr_blocks << "\n";
		cerr << "block size = " << block_size << "\n";

		cache_stream stream(fs.data_dev, block_size, fs.cache_mem);
		duplicate_detector detector(block_size, nr_blocks);

		auto_ptr<progress_monitor> pbar = create_progress_bar("Examining data");
		do {
			chunk const &c = stream.get();
			detector.examine(c);
			pbar->update_percent((stream.index() * 100) / stream.nr_chunks());

		} while (stream.advance());
		pbar->update_percent(100);

		cout << "\n\ntotal dups: " << detector.get_total_duplicates() << endl;
		cout << (detector.get_total_duplicates() * 100) / nr_blocks << "% duplicates\n";

		return 0;
	}

	int show_dups(flags const &fs) {
		if (fs.metadata_dev)
			return show_dups_pool(fs);
		else {
			cerr << "No metadata device provided, so treating data device as a linear device\n";
			return show_dups_linear(fs);
		}
	}

	void usage(ostream &out, string const &cmd) {
		out << "Usage: " << cmd << " [options] {device|file}\n"
		    << "Options:\n"
		    << "  {--block-sectors} <integer>\n"
		    << "  {--metadata-dev} <path>\n"
		    << "  {-h|--help}\n"
		    << "  {-V|--version}" << endl;
	}
}

int thin_show_dups_main(int argc, char **argv)
{
	int c;
	flags fs;

	char const shortopts[] = "qhV";
	option const longopts[] = {
		{ "block-sectors", required_argument, NULL, 1},
		{ "metadata-dev", required_argument, NULL, 2},
		{ "help", no_argument, NULL, 'h'},
		{ "version", no_argument, NULL, 'V'},
		{ NULL, no_argument, NULL, 0 }
	};

	while ((c = getopt_long(argc, argv, shortopts, longopts, NULL)) != -1) {
		switch(c) {
		case 'h':
			usage(cout, basename(argv[0]));
			return 0;

		case 'V':
			cout << THIN_PROVISIONING_TOOLS_VERSION << endl;
			return 0;

		case 1:
			fs.block_size = 512 * parse_int(optarg, "block sectors");
			break;

		case 2:
			fs.metadata_dev = optarg;
			break;

		default:
			usage(cerr, basename(argv[0]));
			return 1;
		}
	}

	if (argc == optind) {
		cerr << "No data device/file provided." << endl;
		usage(cerr, basename(argv[0]));
		exit(1);
	}

	fs.data_dev = argv[optind];

	return show_dups(fs);
}

base::command thin_provisioning::thin_show_dups_cmd("thin_show_duplicates", thin_show_dups_main);

//----------------------------------------------------------------

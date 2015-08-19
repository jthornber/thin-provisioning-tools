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
#include "persistent-data/file_utils.h"
#include "persistent-data/space-maps/core.h"
#include "persistent-data/space-maps/disk.h"
#include "thin-provisioning/commands.h"
#include "thin-provisioning/device_tree.h"
#include "thin-provisioning/mapping_tree.h"
#include "thin-provisioning/superblock.h"

#include <boost/uuid/sha1.hpp>
#include <boost/lexical_cast.hpp>
#include <vector>

using namespace base;
using namespace std;
using namespace thin_provisioning;

//----------------------------------------------------------------

namespace {
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
			: block_size(512 * 1024),
			  cache_mem(64 * 1024 * 1024) {
		}

		unsigned block_size;
		unsigned cache_mem;
	};

	int open_file(string const &path) {
		int fd = ::open(path.c_str(), O_RDONLY | O_DIRECT | O_EXCL, 0666);
		if (fd < 0)
			syscall_failed("open",
				 "Note: you cannot run this tool with these options on live metadata.");

		return fd;
	}

	class duplicate_counter {
	public:
		duplicate_counter(block_address nr_blocks)
		: counts_(nr_blocks),
		  total_dups_(0) {
		}

		void add_duplicate(block_address b1, block_address b2) {
			// cout << "block " << b2 << " is a duplicate of " << b1 << "\n";
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

	int show_dups(string const &path, flags const &fs) {
		cerr << "path = " << path << "\n";
		cerr << "block size = " << fs.block_size << "\n";
		block_address nr_blocks = get_nr_blocks(path, fs.block_size);
		cerr << "nr_blocks = " << nr_blocks << "\n";

		// The cache uses a LRU eviction policy, which plays badly
		// with a sequential read.  So we can't prefetch all the
		// blocks.

		// FIXME: add MRU policy to cache
		unsigned cache_blocks = (fs.cache_mem / fs.block_size) / 2;
		int fd = open_file(path);
		sector_t block_sectors = fs.block_size / 512;
		block_cache cache(fd, block_sectors, nr_blocks, fs.cache_mem);
		validator::ptr v(new bcache::noop_validator());

		duplicate_detector detector(fs.block_size, nr_blocks);

		// warm up the cache
		for (block_address i = 0; i < cache_blocks; i++)
			cache.prefetch(i);

		auto_ptr<progress_monitor> pbar = create_progress_bar("Examining data");

		for (block_address i = 0; i < nr_blocks; i++) {
			block_cache::block &b = cache.get(i, 0, v);
			block_address prefetch = i + cache_blocks;
			if (prefetch < nr_blocks)
				cache.prefetch(prefetch);

			detector.examine(b);
			b.put();

			pbar->update_percent(i * 100 / nr_blocks);
		}

		cout << "\n\ntotal dups: " << detector.get_total_duplicates() << endl;

		return 0;
	}

	void usage(ostream &out, string const &cmd) {
		out << "Usage: " << cmd << " [options] {device|file}\n"
		    << "Options:\n"
		    << "  {--block-sectors} <integer>\n"
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

	return show_dups(argv[optind], fs);
}

base::command thin_provisioning::thin_show_dups_cmd("thin_show_duplicates", thin_show_dups_main);

//----------------------------------------------------------------

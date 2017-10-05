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
#include "thin-provisioning/fixed_chunk_stream.h"
#include "thin-provisioning/pool_stream.h"
#include "thin-provisioning/commands.h"
#include "thin-provisioning/device_tree.h"
#include "thin-provisioning/mapping_tree.h"
#include "thin-provisioning/metadata.h"
#include "thin-provisioning/rmap_visitor.h"
#include "thin-provisioning/superblock.h"
#include "thin-provisioning/variable_chunk_stream.h"

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
		return (n % f) == 0;
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
			: cache_mem(64 * 1024 * 1024),
			  content_based_chunks(false) {
		}

		string data_dev;
		optional<string> metadata_dev;
		optional<unsigned> block_size;
		unsigned cache_mem;
		bool content_based_chunks;
	};

	using namespace mapping_tree_detail;

	class duplicate_counter {
	public:
		duplicate_counter()
		: non_zero_dups_(0),
		  zero_dups_(0) {
		}

		void add_duplicate(block_address len) {
			non_zero_dups_ += len;
		}

		void add_zero_duplicate(block_address len) {
			zero_dups_ += len;
		}

		block_address get_total() const {
			return non_zero_dups_ + zero_dups_;
		}

		block_address get_non_zeroes() const {
			return non_zero_dups_;
		}

		block_address get_zeroes() const {
			return zero_dups_;
		}

		void display_results(chunk_stream const &stream) const {
			block_address meg = 1024 * 1024;
			cout << "\n\n"
			     << stream.size() / meg << "m examined, "
			     << get_non_zeroes() / meg << "m duplicates, "
			     << get_zeroes() / meg << "m zeroes\n";
		}

	private:
		block_address non_zero_dups_;
		block_address zero_dups_;
	};

	class duplicate_detector {
	public:
		void scan_with_variable_sized_chunks(chunk_stream &stream) {
			variable_chunk_stream vstream(stream, 4096);
			scan(vstream);
		}

		void scan_with_fixed_sized_chunks(chunk_stream &stream, block_address chunk_size) {
			fixed_chunk_stream fstream(stream, chunk_size);
			scan(fstream);
		}

		duplicate_counter const &get_results() const {
			return results_;
		}

	private:
		void scan(chunk_stream &stream) {
			block_address total_seen(0);
			unique_ptr<progress_monitor> pbar = create_progress_bar("Examining data");

			do {
				// FIXME: use a wrapper class to automate the put()
				chunk const &c = stream.get();
				examine(c);
				stream.put(c);

				total_seen += c.len_;
				pbar->update_percent((total_seen * 100) / stream.size());

			} while (stream.next());

			pbar->update_percent(100);
			results_.display_results(stream);
		}

		void examine(chunk const &c) {
			if (all_zeroes(c))
				results_.add_zero_duplicate(c.len_);

			else {
				digestor_.reset();
				digestor_.process_bytes(c.mem_.begin, c.mem_.end - c.mem_.begin);

				unsigned int digest[5];
				digestor_.get_digest(digest);

				// hack
				vector<unsigned int> v(5);
				for (unsigned i = 0; i < 5; i++)
					v[i] = digest[i];

				fingerprint_map::const_iterator it = fm_.find(v);
				if (it != fm_.end()) {
					results_.add_duplicate(c.len_);
				} else
					fm_.insert(make_pair(v, c.offset_));
			}
		}

		bool all_zeroes(chunk const &c) const {
			for (uint8_t *ptr = c.mem_.begin; ptr != c.mem_.end; ptr++) {
				if (*ptr != 0)
					return false;
			}

			return true;
		}

		typedef map<vector<unsigned int>, block_address> fingerprint_map;

		unsigned block_size_;
		boost::uuids::detail::sha1 digestor_;
		fingerprint_map fm_;
		duplicate_counter results_;
	};

	int show_dups_pool(flags const &fs) {
		block_manager<>::ptr bm = open_bm(*fs.metadata_dev);
		transaction_manager::ptr tm =
			open_tm(bm, superblock_detail::SUPERBLOCK_LOCATION);
		superblock_detail::superblock sb = read_superblock(bm);
		block_address block_size = sb.data_block_size_ * 512;
		block_address nr_blocks = get_nr_blocks(fs.data_dev, block_size);

		cache_stream stream(fs.data_dev, block_size, fs.cache_mem);
		pool_stream pstream(stream, tm, sb, nr_blocks);

		duplicate_detector detector;

		if (fs.content_based_chunks)
			detector.scan_with_variable_sized_chunks(pstream);
		else {
			if (*fs.block_size) {
				if (factor_of(*fs.block_size, block_size))
					block_size = *fs.block_size;
				else
					throw runtime_error("specified block size is not a factor of the pool chunk size\n");
			}

			detector.scan_with_fixed_sized_chunks(pstream, block_size);
		}

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
		duplicate_detector dd;

		if (fs.content_based_chunks)
			dd.scan_with_variable_sized_chunks(stream);
		else
			dd.scan_with_fixed_sized_chunks(stream, block_size);

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
}

//----------------------------------------------------------------

thin_show_duplicates_cmd::thin_show_duplicates_cmd()
	: command("thin_show_duplicates")
{
}

void
thin_show_duplicates_cmd::usage(std::ostream &out) const
{
	out << "Usage: " << get_name() << " [options] {device|file}\n"
	    << "Options:\n"
	    << "  {--block-sectors} <integer>\n"
	    << "  {--content-based-chunks}\n"
	    << "  {--metadata-dev} <path>\n"
	    << "  {-h|--help}\n"
	    << "  {-V|--version}" << endl;
}

int
thin_show_duplicates_cmd::run(int argc, char **argv)
{
	int c;
	flags fs;

	char const shortopts[] = "qhV";
	option const longopts[] = {
		{ "block-sectors", required_argument, NULL, 1},
		{ "content-based-chunks", no_argument, NULL, 2},
		{ "metadata-dev", required_argument, NULL, 3},
		{ "help", no_argument, NULL, 'h'},
		{ "version", no_argument, NULL, 'V'},
		{ NULL, no_argument, NULL, 0 }
	};

	while ((c = getopt_long(argc, argv, shortopts, longopts, NULL)) != -1) {
		switch(c) {
		case 'h':
			usage(cout);
			return 0;

		case 'V':
			cout << THIN_PROVISIONING_TOOLS_VERSION << endl;
			return 0;

		case 1:
			fs.block_size = 512 * parse_int(optarg, "block sectors");
			break;

		case 2:
			fs.content_based_chunks = true;
			break;

		case 3:
			fs.metadata_dev = optarg;
			break;

		default:
			usage(cerr);
			return 1;
		}
	}

	if (argc == optind) {
		cerr << "No data device/file provided." << endl;
		usage(cerr);
		exit(1);
	}

	fs.data_dev = argv[optind];

	return show_dups(fs);
}

//----------------------------------------------------------------

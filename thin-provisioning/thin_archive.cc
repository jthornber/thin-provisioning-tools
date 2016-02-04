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

#include "version.h"

#include "base/application.h"
#include "base/error_state.h"
#include "base/progress_monitor.h"
#include "persistent-data/data-structures/btree_damage_visitor.h"
#include "persistent-data/file_utils.h"
#include "persistent-data/space-maps/core.h"
#include "persistent-data/space-maps/disk.h"
#include "chunker/cache_stream.h"
#include "chunker/fixed_chunk_stream.h"
#include "chunker/pool_stream.h"
#include "chunker/variable_chunk_stream.h"
#include "thin-provisioning/commands.h"
#include "thin-provisioning/device_tree.h"
#include "thin-provisioning/mapping_tree.h"
#include "thin-provisioning/rmap_visitor.h"
#include "thin-provisioning/superblock.h"

#include <array>
#include <boost/lexical_cast.hpp>
#include <boost/optional.hpp>
#include <boost/uuid/sha1.hpp>
#include <deque>
#include <getopt.h>
#include <iostream>
#include <libgen.h>
#include <vector>

using namespace base;
using namespace boost;
using namespace chunker;
using namespace persistent_data;
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

	//--------------------------------

	typedef uint64_t offset_t;

	class stream_description {
	public:
		enum entry_type {
			ZEROES,
			UNMAPPED,
			MAPPED
		};

		struct entry {
			entry(entry_type t, offset_t len)
				: t_(t), len_(len) {
			}

			entry_type t_;
			offset_t len_;
		};

		typedef uint64_t entry_id;


		stream_description()
			: size_(0) {
		}

		void zeroes(offset_t len) {
			append(ZEROES, len);
		}

		void unmapped(offset_t len) {
			append(UNMAPPED, len);
		}

		void mapped(offset_t len, entry_id id) {
			append(MAPPED, len);
		}

		offset_t size() const {
			return size_;
		}

		deque<entry> const &entries() const {
			return entries_;
		}

		// FIXME: remove
		void dump() const {
			cerr << "in dump\n";

			deque<entry>::const_iterator it;

			for (it = entries_.begin(); it != entries_.end(); ++it) {
				switch (it->t_) {
				case ZEROES:
					cerr << "ZEROES";
					break;

				case UNMAPPED:
					cerr << "UNMAPPED";
					break;

				case MAPPED:
					cerr << "MAPPED";
					break;
				}

				cerr << ": " << it->len_ << "\n";
			}
		}

	private:
		void append(entry_type t, offset_t len) {
			entries_.push_back(entry(t, len));
		}

		offset_t size_;
		deque<entry> entries_;
		set<entry_id> chunks_;
	};

	// FIXME: change to std::array
//	typedef vector<uint32_t> fingerprint;
	typedef std::array<uint32_t, 5> fingerprint;
	typedef block_address fingerprint_location;

	class fingerprint_index {
	public:
		typedef map<fingerprint, fingerprint_location> fingerprint_map;

		fingerprint_index()
			: nr_duplicates_(0) {
		}

		optional<fingerprint_location> lookup(fingerprint const &fp) {
			fingerprint_map::const_iterator it = map_.find(fp);
			if (it != map_.end())
				return optional<fingerprint_location>(it->second);
			else
				return optional<fingerprint_location>();
		}

		void insert(fingerprint const &fp, fingerprint_location loc) {
			map_.insert(make_pair(fp, loc));
		}

	private:
		unsigned nr_duplicates_;
		fingerprint_map map_;
	};

	class chunk_store {
	public:
		fingerprint_location append(fingerprint const &fp, void *data_begin, void *data_end) {
			return 0;
		}
	};

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

	using namespace mapping_tree_detail;

	class scanner {
	public:
		void scan(chunk_stream &stream) {
			variable_chunk_stream vstream(stream, 4096);
			scan_(vstream);
		}

		void dump_stream() const {
			stream_.dump();
		}

	private:
		void scan_(chunk_stream &stream) {
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
		}

		void examine(chunk const &c) {
			if (all_zeroes(c))
				stream_.zeroes(c.len_);

			else {
				digestor_.reset();
				digestor_.process_bytes(c.mem_.begin, c.mem_.end - c.mem_.begin);

				unsigned int digest[5];
				digestor_.get_digest(digest);

				// hack
				fingerprint fp;
				for (unsigned i = 0; i < 5; i++)
				       fp[i] = digest[i];

				optional<fingerprint_location> loc = fp_index_.lookup(fp);

				if (!loc) {
					loc = chunk_store_.append(fp, NULL, NULL);
					fp_index_.insert(fp, *loc);
				}
				stream_.mapped(c.len_, *loc);
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
		fingerprint_index fp_index_;
		stream_description stream_;
		chunk_store chunk_store_;
	};

	int show_dups_pool(flags const &fs) {
		block_manager<>::ptr bm = open_bm(*fs.metadata_dev);
		transaction_manager::ptr tm = open_tm(bm);
		superblock_detail::superblock sb = read_superblock(bm);
		block_address block_size = sb.data_block_size_ * 512;
		block_address nr_blocks = get_nr_blocks(fs.data_dev, block_size);

		cache_stream stream(fs.data_dev, block_size, fs.cache_mem);
		pool_stream pstream(stream, tm, sb, nr_blocks);

		scanner s;
		s.scan(pstream);

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

		scanner s;
		s.scan(stream);
		s.dump_stream();

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

thin_archive_cmd::thin_archive_cmd()
	: command("thin_archive")
{
}

void
thin_archive_cmd::usage(ostream &out) const
{
	out << "Usage: " << get_name() << " [options] {device|file}\n"
	    << "Options:\n"
	    << "  {--block-sectors} <integer>\n"
	    << "  {--metadata-dev} <path>\n"
	    << "  {-h|--help}\n"
	    << "  {-V|--version}" << endl;
}

int
thin_archive_cmd::run(int argc, char **argv)
{
	int c;
	flags fs;

	char const shortopts[] = "qhV";
	option const longopts[] = {
		{ "block-sectors", required_argument, NULL, 1},
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
			fs.block_size = 512 * parse_uint64(optarg, "block sectors");
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

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
#include "thin-provisioning/commands.h"
#include "thin-provisioning/device_tree.h"
#include "thin-provisioning/mapping_tree.h"
#include "thin-provisioning/superblock.h"
#include "thin-provisioning/rmap_visitor.h"

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

	int open_file(string const &path) {
		int fd = ::open(path.c_str(), O_RDONLY | O_DIRECT | O_EXCL, 0666);
		if (fd < 0)
			syscall_failed("open",
				 "Note: you cannot run this tool with these options on live metadata.");

		return fd;
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

	// Once we start using variable sized blocks we will find we want
	// to examine data that crosses cache block boundaries.  So a block
	// to be examined can be composed of multiple chunks of memory.

	struct mem {
		mem(uint8_t *b, uint8_t *e)
			: begin(b),
			  end(e) {
		}

		uint8_t *begin, *end;
	};

	struct chunk {
		// FIXME: switch to bytes rather than sectors
		// FIXME: add length too
		sector_t offset_sectors_;
		deque<mem> mem_;
	};

	class chunk_stream {
	public:
		virtual ~chunk_stream() {}

		virtual block_address nr_chunks() const = 0;
		virtual void rewind() = 0;
		virtual bool advance(block_address count = 1ull) = 0;
		virtual block_address index() const = 0;
		virtual chunk const &get() const = 0;
	};

	class cache_stream : public chunk_stream {
	public:
		cache_stream(string const &path,
			     block_address block_size,
			     size_t cache_mem)
			: block_size_(block_size),
			  nr_blocks_(get_nr_blocks(path, block_size)),

			  // hack because cache uses LRU rather than MRU
			  cache_blocks_((cache_mem / block_size) / 2u),
			  fd_(open_file(path)),
			  v_(new bcache::noop_validator()),
			  cache_(new block_cache(fd_, block_size / 512, nr_blocks_, cache_mem)),
			  current_index_(0) {
			load(0);
			for (block_address i = 1; i < min(cache_blocks_, nr_blocks_); i++)
				cache_->prefetch(i);
		}

		virtual block_address nr_chunks() const {
			return nr_blocks_;
		}

		virtual void rewind() {
			load(0);
		}

		virtual bool advance(block_address count = 1ull) {
			if (current_index_ + count >= nr_blocks_)
				return false;

			current_index_ += count;

			load(current_index_);
			return true;
		}

		virtual block_address index() const {
			return current_index_;
		}

		virtual chunk const &get() const {
			return current_chunk_;
		}

	private:
		void load(block_address b) {
			current_index_ = b;
			current_block_ = cache_->get(current_index_, 0, v_);

			current_chunk_.offset_sectors_ = (b * block_size_) / 512;
			current_chunk_.mem_.clear();
			current_chunk_.mem_.push_back(mem(static_cast<uint8_t *>(current_block_.get_data()),
							  static_cast<uint8_t *>(current_block_.get_data()) + block_size_));

			if (current_index_ + cache_blocks_ < nr_blocks_)
				cache_->prefetch(current_index_ + cache_blocks_);
		}

		block_address block_size_;
		block_address nr_blocks_;
		block_address cache_blocks_;
		int fd_;
		validator::ptr v_;
		auto_ptr<block_cache> cache_;

		block_address current_index_;
		block_cache::auto_block current_block_;
		chunk current_chunk_;
	};

	//--------------------------------

	typedef rmap_visitor::region region;
	typedef rmap_visitor::rmap_region rmap_region;

	uint32_t const UNMAPPED = -1;

	class pool_stream : public chunk_stream {
	public:

		pool_stream(cache_stream &stream,
			    transaction_manager::ptr tm, superblock_detail::superblock const &sb,
			    block_address nr_blocks)
			: stream_(stream),
			  block_to_thin_(stream.nr_chunks(), UNMAPPED),
			  nr_mapped_(0) {
			init_rmap(tm, sb, nr_blocks);
		}

		block_address nr_chunks() const {
			return stream_.nr_chunks();
		}

		void rewind() {
			stream_.rewind();
		}

		bool advance(block_address count = 1ull) {
			while (count--)
				if (!advance_one())
					return false;

			return true;
		}

		block_address index() const {
			return stream_.index();
		}

		chunk const &get() const {
			return stream_.get();
		}

	private:
		class damage_visitor {
		public:
			virtual void visit(btree_path const &path, btree_detail::damage const &d) {
				throw std::runtime_error("damage in mapping tree, please run thin_check");
			}
		};

		// FIXME: too big to return by value
		vector<rmap_region> read_rmap(transaction_manager::ptr tm, superblock_detail::superblock const &sb,
					      block_address nr_blocks) {
			damage_visitor dv;
			rmap_visitor rv;

			mapping_tree mtree(*tm, sb.data_mapping_root_,
					   mapping_tree_detail::block_traits::ref_counter(tm->get_sm()));

			rv.add_data_region(rmap_visitor::region(0, nr_blocks));

			btree_visit_values(mtree, rv, dv);
			rv.complete();
			cerr << "rmap size: " << rv.get_rmap().size() << "\n";
			return rv.get_rmap();
		}

		void init_rmap(transaction_manager::ptr tm, superblock_detail::superblock const &sb,
			       block_address nr_blocks) {
			cerr << "reading rmap...";
			vector<rmap_region> rmap = read_rmap(tm, sb, nr_blocks);
			cerr << "done\n";

			vector<rmap_region>::const_iterator it;
			set<uint32_t> thins;
			for (it = rmap.begin(); it != rmap.end(); ++it) {
				rmap_region const &r = *it;
				for (block_address b = r.data_begin; b != r.data_end; b++)
					if (block_to_thin_[b] == UNMAPPED) {
						nr_mapped_++;
						block_to_thin_[b] = r.thin_dev;
					}
				thins.insert(r.thin_dev);
			}

			cerr << nr_mapped_ << " mapped blocks\n";
			cerr << "there are " << thins.size() << " thin devices\n";
		}

		bool advance_one() {
			block_address new_index = index() + 1;

			while (block_to_thin_[new_index] == UNMAPPED &&
			       new_index < nr_chunks())
				new_index++;

			if (new_index >= nr_chunks())
				return false;

			return stream_.advance(new_index - index());
		}

		cache_stream &stream_;
		vector<uint32_t> block_to_thin_;
		block_address nr_mapped_;
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
//		cout << (detector.get_total_duplicates() * 100) / nr_mapped_ << "% duplicates\n";

		return 0;
	}

	int show_dups_linear(flags const &fs) {
		if (!fs.block_size)
			// FIXME: this check should be moved to the switch parsing
			throw runtime_error("--block-sectors or --metadata-dev must be supplied");

		cerr << "path = " << fs.data_dev << "\n";
		cerr << "block size = " << fs.block_size << "\n";
		block_address nr_blocks = get_nr_blocks(fs.data_dev, *fs.block_size);
		cerr << "nr_blocks = " << nr_blocks << "\n";

		// The cache uses a LRU eviction policy, which plays badly
		// with a sequential read.  So we can't prefetch all the
		// blocks.

		// FIXME: add MRU policy to cache
		unsigned cache_blocks = (fs.cache_mem / *fs.block_size) / 2;
		int fd = open_file(fs.data_dev);
		sector_t block_sectors = *fs.block_size / 512;
		block_cache cache(fd, block_sectors, nr_blocks, fs.cache_mem);
		validator::ptr v(new bcache::noop_validator());

		duplicate_detector detector(*fs.block_size, nr_blocks);

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

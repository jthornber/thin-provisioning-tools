// Copyright (C) 2018 Red Hat, Inc. All rights reserved.
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
#include <fcntl.h>

#include <boost/lexical_cast.hpp>
#include <boost/optional.hpp>

#include <map>
#include <set>

#include "version.h"

#include "base/application.h"
#include "base/error_state.h"
#include "base/file_utils.h"
#include "base/nested_output.h"
#include "persistent-data/data-structures/btree_counter.h"
#include "persistent-data/space-maps/core.h"
#include "persistent-data/space-maps/disk.h"
#include "persistent-data/file_utils.h"
#include "thin-provisioning/metadata.h"
#include "thin-provisioning/device_tree.h"
#include "thin-provisioning/mapping_tree.h"
#include "thin-provisioning/metadata_counter.h"
#include "thin-provisioning/superblock.h"
#include "thin-provisioning/commands.h"
#include "thin-provisioning/thin_journal.h"

using namespace base;
using namespace boost;
using namespace file_utils;
using namespace std;
using namespace thin_provisioning;

//----------------------------------------------------------------

namespace {

	unsigned const MAX_HELD_LOCKS = 16;

	// We use a temporary file to hold all the deltas.  Assume the metadata initially starts zeroed.
	// Need to introduce notion of 'time' that increments everytime a write lock is taken.
	// Need to track updates to the superblock to define transactions.
	class checker : public journal_visitor {
	public:
		checker(block_address &nr_metadata_blocks)
			: bm_(new block_manager<>("metadata.tmp", nr_metadata_blocks, MAX_HELD_LOCKS, block_manager<>::CREATE)) {
		}

		virtual void visit(read_lock_msg const &msg) {
			read_lock_(msg.index_);
		}

		virtual void visit(write_lock_msg const &msg) {
			write_lock_(msg.index_);
		}

		virtual void visit(zero_lock_msg const &msg) {
			write_lock_(msg.index_);
		}

		virtual void visit(try_read_lock_msg const &msg) {
			read_lock_(msg.index_);
		}

		virtual void visit(unlock_msg const &msg) {
			unlock_(msg.index_, msg.deltas_);
		}

		virtual void visit(verify_msg const &msg) {
			// noop
		}

		virtual void visit(prepare_msg const &msg) {
			// noop
		}

		virtual void visit(flush_msg const &msg) {
			cerr << "spurious flush()\n";
		}

		virtual void visit(flush_and_unlock_msg const &msg) {
			if (msg.index_ != superblock_detail::SUPERBLOCK_LOCATION) {
				cerr << "flush_and_unlock received for block " << msg.index_
				     << ", which isn't the superblock\n";
				throw runtime_error("bad flush_and_unlock");
			}

			commit(msg.deltas_);
		}

		virtual void visit(prefetch_msg const &msg) {
			// ignore
		}

		virtual void visit(set_read_only_msg const &msg) {
			// ignore
		}

		virtual void visit(set_read_write_msg const &msg) {
			// ignore
		}

	private:
		void read_lock_(block_address b) {
			if (write_locks_.count(b)) {
				cerr << "read lock taken concurrently with write lock for block "
				     << b << "\n";
				throw runtime_error("bad read lock");
			}

			auto it = read_locks_.find(b);
			if (it == read_locks_.end())
				read_locks_.insert(make_pair(b, 1));
			else
				it->second++;
		}

		void write_lock_(block_address b) {
			if (active_.count(b)) {
				cerr << "write lock taken for block "
				     << b
				     << ", but it is still in the active transaction\n";
				throw runtime_error("bad write lock");
			}

			if (write_locks_.count(b)) {
				cerr << "write lock already held for block "
				     << b
				     << "\n";
				throw runtime_error("bad write lock");
			}

			if (read_locks_.count(b)) {
				cerr << "read lock requested for write locked block "
				     << b << "\n";
				throw runtime_error("bad write lock");
			}

			write_locks_.insert(b);
		}


		void unlock_(block_address b, delta_list const &deltas) {
			if (write_locks_.count(b)) {
				write_locks_.erase(b);

				auto wr = bm_->write_lock(b);

				for (auto &&d : deltas) {
					uint8_t *data = static_cast<uint8_t *>(wr.data());

					if (d.offset_ + d.bytes_.size() > 4096) {
						cerr << "delta for block " << b << " is out of range ("
                                                     << d.offset_ << ", " << d.offset_ + d.bytes_.size() << "]\n";
                                                throw runtime_error("bad unlock");
                                        }

					memcpy(data + d.offset_, d.bytes_.data(), d.bytes_.size());
				}

			} else {
				auto it = read_locks_.find(b);
				if (it == read_locks_.end()) {
					cerr << "unlock requested on block " << b << ", which isn't locked\n";
					throw runtime_error("bad unlock");
				}

				if (deltas.size()) {
					cerr << "unlocking a read lock for " << b << ", yet there are " << deltas.size() << " deltas\n";
					throw runtime_error("bad unlock");
				}

				// Decrement lock
				if (!it->second) {
					cerr << "read lock entry has zero count (internal error)\n";
					throw runtime_error("bad unlock");
				}

				if (!--it->second)
					read_locks_.erase(it);

			}
		}

		void commit(delta_list const &deltas) {
			// At this point the only lock held should be the superblock,
			// and that should be a write lock.
			if (read_locks_.size()) {
				cerr << "committing when the following read locks are still held:\n";
				for (auto &&p : read_locks_)
					cerr << p.first << "\n";
			}


			unlock_(superblock_detail::SUPERBLOCK_LOCATION, deltas);

			if (write_locks_.size()) {
				cerr << "commit() called, but the following write locks are held:\n";
				for (auto &&b : write_locks_)
					cerr << b << "\n";
			}

			build_active_set_();
		}

		void build_active_set_() {
			using namespace thin_provisioning::superblock_detail;

			superblock sb = read_superblock(bm_);
			block_counter bc;

			auto tm = open_tm(bm_, SUPERBLOCK_LOCATION);
			auto sm = open_metadata_sm(*tm, &sb.metadata_space_map_root_);
			tm->set_sm(sm);

			// FIXME: check we don't have a space leak from a cycle between the sm and tm

			count_metadata(tm, sb, bc);

			active_.clear();
			active_.insert(SUPERBLOCK_LOCATION);

			for (auto &&p : bc.get_counts()) {
				if (!p.second) {
					cerr << "weird zero count for block " << p.first << "\n";
					throw runtime_error("build_active_set() failed");
				}

				active_.insert(p.first);
			}
		}

		typedef set<block_address> block_set;
		typedef map<block_address, unsigned> block_map;

		block_set active_;
		block_set write_locks_;
		block_map read_locks_;

		block_manager<>::ptr bm_;
		transaction_manager::ptr tm_;
	};

	struct flags {
		flags()
			: quiet(false) {
		}

		bool quiet;
	};

	void check(string const &path, block_address nr_metadata_blocks) {
		block_address journal_size = get_file_length(path) / JOURNAL_BLOCK_SIZE;
		block_manager<JOURNAL_BLOCK_SIZE>::ptr bm(
			new block_manager<JOURNAL_BLOCK_SIZE>(path, journal_size, 4,
                                          block_manager<JOURNAL_BLOCK_SIZE>::READ_ONLY));
		journal j(bm);
		checker c(nr_metadata_blocks);

		j.read_journal(c);
	}
}

//----------------------------------------------------------------

thin_journal_cmd::thin_journal_cmd()
	: command("thin_journal_check")
{
}

void
thin_journal_cmd::usage(std::ostream &out) const
{
	out << "Usage: " << get_name() << " [options] {device|file} {nr blocks}" << endl
	    << "Options:\n"
	    << "  {-q|--quiet}\n"
	    << "  {-h|--help}\n"
	    << "  {-V|--version}\n";
}

int
thin_journal_cmd::run(int argc, char **argv)
{
	int c;
	flags fs;

	char const shortopts[] = "qhV";
	option const longopts[] = {
		{ "quiet", no_argument, NULL, 'q'},
		{ "help", no_argument, NULL, 'h'},
		{ "version", no_argument, NULL, 'V'},
		{ NULL, no_argument, NULL, 0 }
	};

	while ((c = getopt_long(argc, argv, shortopts, longopts, NULL)) != -1) {
		switch(c) {
		case 'h':
			usage(cout);
			return 0;

		case 'q':
			fs.quiet = true;
			break;

		case 'V':
			cout << THIN_PROVISIONING_TOOLS_VERSION << endl;
			return 0;

		default:
			usage(cerr);
			return 1;
		}
	}

	if (argc - optind != 2) {
		if (!fs.quiet)
			usage(cerr);

		exit(1);
	}

	try {
		check(argv[optind], lexical_cast<block_address>(argv[optind + 1]));

	} catch (std::exception &e) {
		cerr << e.what() << "\n";
		return 1;
	}

	return 0;
}

//----------------------------------------------------------------

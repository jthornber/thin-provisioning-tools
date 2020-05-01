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
	class journal_display : public journal_visitor {
	public:
		journal_display(journal_visitor &inner)
			: inner_(inner) {
		}

		virtual void visit(open_journal_msg const &msg) {
			cout << "open_journal\n";
			inner_.visit(msg);
		}

		virtual void visit(close_journal_msg const &msg) {
			cout << "close_journal\n";
			inner_.visit(msg);
		}

		virtual void visit(read_lock_msg const &msg) {
			if (interesting(msg.index_))
				cout << "read_lock " << msg.index_ << "\n";
			inner_.visit(msg);
		}

		virtual void visit(write_lock_msg const &msg) {
			if (interesting(msg.index_))
				cout << "write_lock " << msg.index_ << "\n";
			inner_.visit(msg);
		}

		virtual void visit(zero_lock_msg const &msg) {
			if (interesting(msg.index_))
				cout << "zero_lock " << msg.index_ << "\n";
			inner_.visit(msg);
		}

		virtual void visit(try_read_lock_msg const &msg) {
			if (interesting(msg.index_))
				cout << "try_read_lock " << msg.index_ << "\n";
			inner_.visit(msg);
		}

		virtual void visit(unlock_msg const &msg) {
			if (interesting(msg.index_))
				cout << "unlock " << msg.index_ << "\n";
			inner_.visit(msg);
		}

		virtual void visit(verify_msg const &msg) {
			if (interesting(msg.index_))
				cout << "verify " << msg.index_ << "\n";
			inner_.visit(msg);
		}

		virtual void visit(prepare_msg const &msg) {
			if (interesting(msg.index_))
				cout << "prepare " << msg.index_ << "\n";
			inner_.visit(msg);
		}

		virtual void visit(flush_msg const &msg) {
			cout << "flush\n";
			inner_.visit(msg);
		}

		virtual void visit(flush_and_unlock_msg const &msg) {
			if (interesting(msg.index_))
				cout << "flush_and_unlock " << msg.index_ << "\n";
			inner_.visit(msg);
		}

		virtual void visit(prefetch_msg const &msg) {
			if (interesting(msg.index_))
				cout << "prefetch " << msg.index_ << "\n";
			inner_.visit(msg);
		}

		virtual void visit(set_read_only_msg const &msg) {
			cout << "set_read_only\n";
			inner_.visit(msg);
		}

		virtual void visit(set_read_write_msg const &msg) {
			cout << "set_read_write\n";
			inner_.visit(msg);
		}

		bool interesting(block_address b) const {
			return true;
		}

		journal_visitor &inner_;
	};

	unsigned const MAX_HELD_LOCKS = 16;

	// We use a temporary file to hold all the deltas.  Assume the metadata initially starts zeroed.
	// Need to introduce notion of 'time' that increments everytime a write lock is taken.
	// Need to track updates to the superblock to define transactions.
	class checker : public journal_visitor {
	public:
		virtual void visit(open_journal_msg const &msg) {
			bm_.reset(new block_manager("metadata.tmp", msg.nr_metadata_blocks_,
                                                    MAX_HELD_LOCKS, block_manager::CREATE));
		}

		virtual void visit(close_journal_msg const &msg) {
			// noop
		}

		virtual void visit(read_lock_msg const &msg) {
			if (msg.success_)
				read_lock_(msg.index_);
		}

		virtual void visit(write_lock_msg const &msg) {
			if (msg.success_)
				write_lock_(msg.index_);
		}

		virtual void visit(zero_lock_msg const &msg) {
			if (msg.success_) {
				write_lock_(msg.index_);
				zero_(msg.index_);
			}
		}

		virtual void visit(try_read_lock_msg const &msg) {
			if (msg.success_)
				read_lock_(msg.index_);
		}

		virtual void visit(unlock_msg const &msg) {
			bool write_locked = is_write_locked_(msg.index_);

			unlock_(msg.index_, msg.deltas_);

			if (write_locked && msg.index_ == superblock_detail::SUPERBLOCK_LOCATION)
				commit_();
		}

		virtual void visit(verify_msg const &msg) {
			// noop
		}

		virtual void visit(prepare_msg const &msg) {
			// noop
		}

		virtual void visit(flush_msg const &msg) {
			cout << "WARN: spurious flush()\n";
		}

		virtual void visit(flush_and_unlock_msg const &msg) {
			if (msg.index_ != superblock_detail::SUPERBLOCK_LOCATION) {
				cout << "ERROR: flush_and_unlock received for block " << msg.index_
				     << ", which isn't the superblock\n";
			}

			unlock_(msg.index_, msg.deltas_);
			commit_();
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
			auto it = locks_.find(b);
			if (it == locks_.end())
				locks_.insert(make_pair(b, -1));

			else if (it->second > 0) {
				cout << "WARN: read lock taken concurrently with write lock for block "
				     << b << "\n";

			} else
				--it->second;
		}

		void write_lock_(block_address b) {
			if (is_superblock_(b)) {
				if (locks_.size())
					cout << "WARN: superblock taken when locks still held\n";

			} else if (active_.count(b)) {
				cout << "ERROR: write lock taken for block "
				     << b
				     << ", but it is still in the active transaction\n";
				throw runtime_error("bad write_lock");
			}

			auto it = locks_.find(b);
			if (it == locks_.end())
				locks_.insert(make_pair(b, 1));

			else if (it->second < 0) {
				cout << "WARN: write lock requested for read locked block "
				     << b << "\n";
			} else
				it->second++;
		}

		bool is_write_locked_(block_address b) const {
			auto it = locks_.find(b);
			return it != locks_.end() && it->second > 0;
		}

		void unlock_(block_address b, delta_list const &deltas) {
			auto it = locks_.find(b);
			if (it == locks_.end() || !it->second) {
				cout << "ERROR: unlock requested on block " << b << ", which isn't locked\n";
				throw runtime_error("bad unlock");
			}

			if (it->second < 0) {
				it->second++;

				if (deltas.size()) {
					cout << "ERROR: unlocking a read lock for " << b << ", yet there are " << deltas.size() << " deltas\n";
					throw runtime_error("bad unlock");
				}
			} else {
				auto wr = bm_->write_lock(b);

				for (auto &&d : deltas) {
					uint8_t *data = static_cast<uint8_t *>(wr.data());

					if (d.offset_ + d.bytes_.size() > 4096) {
						cout << "ERROR: delta for block " << b << " is out of range ("
                                                     << d.offset_ << ", " << d.offset_ + d.bytes_.size() << "]\n";
                                                throw runtime_error("bad unlock");
                                        }

					memcpy(data + d.offset_, d.bytes_.data(), d.bytes_.size());
				}

				it->second--;
			}

			if (!it->second)
				locks_.erase(it);
		}

		void zero_(block_address b) {
			auto wr = bm_->write_lock_zero(b);
		}

		void commit_() {
			using namespace thin_provisioning::superblock_detail;

			// At this point the only lock held should be the superblock,
			// and that should be a write lock.
			if (locks_.size() != 0) {
				cout << "ERROR: committing when the following locks are still held:\n";
				for (auto &&p : locks_)
					if (p.first != SUPERBLOCK_LOCATION)
						cerr << p.first << "\n";
				throw runtime_error("bad commit");
			}

			build_active_set_();
		}

		void build_active_set_() {
			using namespace thin_provisioning::superblock_detail;

			cerr << "build active set\n";
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
					cout << "weird zero count for block " << p.first << "\n";
				}

				active_.insert(p.first);
			}
		}

		bool is_superblock_(block_address b) const {
			return b == superblock_detail::SUPERBLOCK_LOCATION;
		}

		typedef set<block_address> block_set;

		// write locks positive, unlocked 0, read locks negative
		typedef map<block_address, int> block_map;

		block_set active_;
		block_map locks_;

		block_manager::ptr bm_;
		transaction_manager::ptr tm_;
	};

	struct flags {
		flags()
			: quiet(false) {
		}

		bool quiet;
	};

	void check(string const &path) {
		block_address journal_size = get_file_length(path) / JOURNAL_BLOCK_SIZE;
		block_manager::ptr bm(
			new block_manager(path, journal_size, 4,
                                          block_manager::READ_ONLY));
		journal j(bm);
		checker c;
		journal_display dc(c);

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
	out << "Usage: " << get_name() << " [options] {device|file}" << endl
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

	if (argc - optind != 1) {
		if (!fs.quiet)
			usage(cerr);

		exit(1);
	}

	try {
		check(argv[optind]);

	} catch (std::exception &e) {
		cerr << e.what() << "\n";
		return 1;
	}

	return 0;
}

//----------------------------------------------------------------

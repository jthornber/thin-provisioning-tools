#include "base/progress_monitor.h"
#include "persistent-data/file_utils.h"
#include "block-cache/copier.h"
#include "caching/commands.h"
#include "caching/mapping_array.h"
#include "caching/metadata.h"
#include "version.h"

#include <boost/optional.hpp>
#include <getopt.h>
#include <string>
#include <stdexcept>
#include <boost/optional/optional_io.hpp>

using namespace bcache;
using namespace caching;
using namespace boost;
using namespace std;

//----------------------------------------------------------------

namespace {

	template <typename T> T safe_div(T const n, T const d, T const def) {
		return (d == T()) ? def : (n / d);
	}

	//--------------------------------

	struct flags {
		flags()
			: cache_size(4 * 1024 * 1024),
			  sort_buffers(16 * 1024),
			  origin_dev_offset(0),
			  fast_dev_offset(0),
			  list_failed_blocks(false),
			  update_metadata(true) {
		}

		// The sort buffers have a dramatic effect on the
		// performance.  We give up 10% of the general buffer space
		// for them.
		void calc_sort_buffer_size() {
			size_t sbs = cache_size / 10;
			cache_size = cache_size - sbs;

			sort_buffers = sbs / sizeof(copy_op);
		}

		using maybe_string = boost::optional<string>;

		size_t cache_size;
		unsigned sort_buffers;
		maybe_string metadata_dev;
		maybe_string origin_dev;
		maybe_string fast_dev;
		sector_t origin_dev_offset;
		sector_t fast_dev_offset;
		bool list_failed_blocks;
		bool update_metadata;
	};

	//--------------------------------

	class copy_batch {
	public:
		copy_batch(unsigned nr)
		: max_(nr),
		  count_(0),
		  ops_(nr) {
		}

		bool space() const {
			return count_ < max_;
		}

		void push_op(copy_op const &op) {
			if (!space())
				throw runtime_error("copy_batch out of space");

			ops_[count_++] = op;
		}

		void reset() {
			count_ = 0;
		}

		vector<copy_op>::iterator begin() {
			return ops_.begin();
		}

		vector<copy_op>::iterator end() {
			return ops_.begin() + count_;
		}

	private:
		unsigned max_;
		unsigned count_;
		vector<copy_op> ops_;
	};

	class copy_visitor : public mapping_visitor {
	public:
		copy_visitor(copier &c, unsigned sort_buffer, bool only_dirty,
			     bool list_failed_blocks,
			     progress_monitor &monitor, unsigned cache_blocks)
			: copier_(c),
			  block_size_(c.get_block_size()),
			  only_dirty_(only_dirty),
			  batch_(sort_buffer),
			  monitor_(monitor),
			  cache_blocks_(cache_blocks) {
		}

		virtual void visit(block_address cblock, mapping const &m) {
			stats_.blocks_scanned = cblock;
			update_monitor();

			if (!(m.flags_ & M_VALID))
				return;

			if (only_dirty_ && !(m.flags_ & M_DIRTY))
				return;

			copy_op cop;
			cop.src_b = cblock;
			cop.src_e = cblock + 1ull;
			cop.dest_b = m.oblock_;

			// blocks
			stats_.blocks_needed++;
			batch_.push_op(cop);
			if (!batch_.space())
				issue();
		}

		void issue() {
			auto compare_dest = [](copy_op const &lhs, copy_op const &rhs) {
				return lhs.dest_b < rhs.dest_b;
			};
			sort(batch_.begin(), batch_.end(), compare_dest);

			auto e = batch_.end();
			for (auto it = batch_.begin(); it != e; ++it) {
				copier_.issue(*it);
				stats_.blocks_issued++;
				update_monitor();

				check_for_completed_copies();
			}
			check_for_completed_copies();

			batch_.reset();
		}

		void check_for_completed_copies(bool block = false) {
			optional<copy_op> mop;

			do {
				if (block)
					mop = copier_.wait();

				else {
					unsigned micro = 0;
					mop = copier_.wait(micro);
				}

				if (mop) {
					inc_completed(*mop);
					if (!mop->success()) {
						failed_blocks_.insert(*mop);
						failed_cblocks_.insert(mop->src_b);
					}
				}

			} while (mop);
		}

		void complete() {
			issue();

			while (copier_.nr_pending())
				check_for_completed_copies(true);

			monitor_.update_percent(100);
			cerr << "\n";
		}

		void inc_completed(copy_op const &op) {
			stats_.blocks_completed++;
			update_monitor();
		}

		void update_monitor() {
			static unsigned call_count = 0;
			if (call_count++ % 128)
				return;

			::uint64_t scanned = stats_.blocks_scanned * 100 / cache_blocks_;
			::uint64_t copied = safe_div<block_address>(stats_.blocks_completed * 100,
								  stats_.blocks_needed, 100ull);
			::uint64_t percent = min<::uint64_t>(scanned, copied);
			monitor_.update_percent(percent);
		}

		struct copy_stats {
			copy_stats()
				: blocks_scanned(0),
				  blocks_needed(0),
				  blocks_issued(0),
				  blocks_completed(0),
				  blocks_failed(0) {
			}

			block_address blocks_scanned;
			block_address blocks_needed;
			block_address blocks_issued;
			block_address blocks_completed;
			block_address blocks_failed;
		};

		copy_stats const &get_stats() const {
			return stats_;
		}

		set<block_address> failed_writebacks() const {
			return failed_cblocks_;
		}

	private:
		copier &copier_;
		unsigned block_size_;
		bool only_dirty_;

		copy_stats stats_;
		copy_batch batch_;
		progress_monitor &monitor_;
		unsigned cache_blocks_;

		set<copy_op> failed_blocks_;
		set<block_address> failed_cblocks_;
	};

	//--------------------------------

	using namespace mapping_array_damage;

	class ignore_damage_visitor : public damage_visitor {
	public:
		ignore_damage_visitor()
			: corruption_(false) {
		}

		void visit(missing_mappings const &d) {
			cerr << "missing mappings (" << d.keys_.begin_ << ", " << d.keys_.end_ << "]\n";
			corruption_ = true;
		}

		void visit(invalid_mapping const &d) {
			cerr << "invalid mapping cblock = " << d.cblock_ << ", oblock = " << d.m_.oblock_ << "\n";
			corruption_ = true;
		}

		bool was_corruption() const {
			return corruption_;
		}

	private:
		bool corruption_;
	};

	bool clean_shutdown(metadata const &md) {
		return md.sb_.flags.get_flag(superblock_flags::CLEAN_SHUTDOWN);
	}

	void update_metadata(metadata &md, set<block_address> const &failed_writebacks) {
		cout << "Updating metadata ... ";

		cout.flush();

		auto &mappings = md.mappings_;
		for (block_address cblock = 0; cblock < mappings->get_nr_entries(); cblock++) {
			auto m = mappings->get(cblock);
			if (!(m.flags_ & M_VALID))
				continue;

			if (!(m.flags_ & M_DIRTY))
				continue;

			if (failed_writebacks.count(cblock))
				continue;

			m.flags_ &= ~M_DIRTY;
			cerr << "clearing dirty flag for block " << cblock << "\n";
			mappings->set(cblock, m);
		}
		md.commit(true);
		cout << "done\n";
		cout.flush();
	}

	int writeback_(flags const &f) {
		block_manager::ptr bm = open_bm(*f.metadata_dev, block_manager::READ_WRITE);
		metadata md(bm);

		// FIXME: we're going to have to copy runs to get the through put with small block sizes
		unsigned max_ios = f.cache_size / (md.sb_.data_block_size << SECTOR_SHIFT);
		aio_engine engine(max_ios);
		copier c(engine, *f.fast_dev, *f.origin_dev,
			 md.sb_.data_block_size, f.cache_size,
			 f.fast_dev_offset >> SECTOR_SHIFT,
			 f.origin_dev_offset >> SECTOR_SHIFT);

		auto bar = create_progress_bar("Copying data");
		copy_visitor cv(c, f.sort_buffers, clean_shutdown(md), f.list_failed_blocks,
				*bar, md.sb_.cache_blocks);

		ignore_damage_visitor dv;

		walk_mapping_array(*md.mappings_, cv, dv);
		cv.complete();

		auto stats = cv.get_stats();
		cout << stats.blocks_issued - stats.blocks_failed << "/"
		     << stats.blocks_issued << " blocks successfully copied.\n";

		if (stats.blocks_failed)
			cout << stats.blocks_failed << " blocks were not copied\n";

		if (dv.was_corruption()) {
			cout << "Metadata corruption was found, some data may not have been copied.\n";
			if (f.update_metadata)
				cout << "Unable to update metadata.\n";

		} else if (f.update_metadata)
			update_metadata(md, cv.failed_writebacks());

		return (stats.blocks_failed || dv.was_corruption()) ? 1 : 0;
	}

	int writeback(flags const &f) {
		int r;

		try {
			r = writeback_(f);

		} catch (std::exception &e) {
			cerr << e.what() << endl;
			return 1;
		}

		return r;
	}
}

//----------------------------------------------------------------

cache_writeback_cmd::cache_writeback_cmd()
	: command("cache_writeback")
{
}

void
cache_writeback_cmd::usage(std::ostream &out) const
{
	out << "Usage: " << get_name() << " [options]\n"
	    << "\t\t--metadata-device <dev>\n"
	    << "\t\t--origin-device <dev>\n"
	    << "\t\t--fast-device <dev>\n"
	    << "\t\t--buffer-size-meg <size>\n"
	    << "\t\t--list-failed-blocks\n"
	    << "\t\t--no-metadata-update\n"
	    << "\t\t--origin-device-offset <bytes>\n"
	    << "\t\t--fast-device-offset <bytes>\n"
	    << "Options:\n"
	    << "  {-h|--help}\n"
	    << "  {-V|--version}" << endl;
}

int
cache_writeback_cmd::run(int argc, char **argv)
{
	int c;
	flags fs;
	char const *short_opts = "hV";
	option const long_opts[] = {
		{ "metadata-device", required_argument, NULL, 0 },
		{ "origin-device", required_argument, NULL, 1 },
		{ "fast-device", required_argument, NULL, 2 },
		{ "buffer-size-meg", required_argument, NULL, 3 },
		{ "list-failed-blocks", no_argument, NULL, 4 },
		{ "no-metadata-update", no_argument, NULL, 5 },
		{ "origin-device-offset", required_argument, NULL, 6 },
		{ "fast-device-offset", required_argument, NULL, 7 },
		{ "help", no_argument, NULL, 'h'},
		{ "version", no_argument, NULL, 'V'},
		{ NULL, no_argument, NULL, 0 }
	};

	while ((c = getopt_long(argc, argv, short_opts, long_opts, NULL)) != -1) {
		switch(c) {
		case 0:
			fs.metadata_dev = optarg;
			break;

		case 1:
			fs.origin_dev = optarg;
			break;

		case 2:
			fs.fast_dev = optarg;
			break;

		case 3:
			fs.cache_size = parse_uint64(optarg, "buffer size") * 1024 * 1024;
			break;

		case 4:
			fs.list_failed_blocks = true;
			break;

		case 5:
			fs.update_metadata = false;
			break;

		case 6:
			fs.origin_dev_offset = parse_uint64(optarg, "origin dev offset");
			break;

		case 7:
			fs.fast_dev_offset = parse_uint64(optarg, "fast dev offset");
			break;

		case 'h':
			usage(cout);
			return 0;

		case 'V':
			cout << THIN_PROVISIONING_TOOLS_VERSION << endl;
			return 0;

		default:
			usage(cerr);
			return 1;
		}
	}

	fs.calc_sort_buffer_size();

	if (argc != optind) {
		usage(cerr);
		return 1;
	}

        if (!fs.metadata_dev) {
		cerr << "No metadata device provided.\n\n";
		usage(cerr);
		return 1;
	}

	if (!fs.origin_dev) {
		cerr << "No origin device provided.\n\n";
		usage(cerr);
		return 1;
	}

	if (!fs.fast_dev) {
		cerr << "No fast device provided.\n\n";
		usage(cerr);
		return 1;
	}

	if (fs.origin_dev_offset & (SECTOR_SHIFT - 1) ||
	    fs.fast_dev_offset & (SECTOR_SHIFT - 1)) {
		cerr << "Offset must be sector-aligned\n\n";
		usage(cerr);
		return 1;
	}

	return writeback(fs);
}

//----------------------------------------------------------------

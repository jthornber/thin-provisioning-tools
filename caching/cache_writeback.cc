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

using namespace bcache;
using namespace caching;
using namespace boost;
using namespace std;

//----------------------------------------------------------------

namespace {
	struct flags {
		using maybe_string = boost::optional<string>;

		maybe_string metadata_dev;
		maybe_string origin_dev;
		maybe_string fast_dev;
	};

	class copy_visitor : public mapping_visitor {
	public:
		copy_visitor(copier &c, bool only_dirty)
			: copier_(c),
			  block_size_(c.get_block_size()),
			  only_dirty_(only_dirty) {
		}

		virtual void visit(block_address cblock, mapping const &m) {
			if (!(m.flags_ & M_VALID))
				return;

			if (only_dirty_ && !(m.flags_ & M_DIRTY))
				return;

			auto sectors_copied = copier_.copy(cblock, m.oblock_);

			stats_.blocks_issued++;
			if (sectors_copied < block_size_) {
				stats_.blocks_failed++;
				stats_.sectors_failed += block_size_ - sectors_copied;
			}
		}

		struct copy_stats {
			copy_stats()
				: blocks_issued(0),
				  blocks_failed(0),
				  sectors_failed(0) {
			}

			block_address blocks_issued;
			block_address blocks_failed;
			block_address sectors_failed;
		};

		copy_stats const &get_stats() const {
			return stats_;
		}

	private:
		copier &copier_;
		unsigned block_size_;
		bool only_dirty_;
		copy_stats stats_;
	};

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

	int writeback_(flags const &f) {
		block_manager<>::ptr bm = open_bm(*f.metadata_dev, block_manager<>::READ_ONLY);
		metadata md(bm, metadata::OPEN);

		copier c(*f.fast_dev, *f.origin_dev, md.sb_.data_block_size);
		copy_visitor cv(c, clean_shutdown(md));
		ignore_damage_visitor dv;
		walk_mapping_array(*md.mappings_, cv, dv);

		auto stats = cv.get_stats();
		cout << stats.blocks_issued << " copies issued\n"
		     << stats.blocks_failed << " copies failed\n";

		if (stats.blocks_failed)
			cout << stats.sectors_failed << " sectors were not copied\n";

		if (dv.was_corruption())
			cout << "Metadata corruption was found, some data may not have been copied.\n";

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

	return writeback(fs);

}

//----------------------------------------------------------------

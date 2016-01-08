#include <iostream>
#include <getopt.h>
#include <sys/ioctl.h>
#include <linux/fs.h>
#include <libgen.h>

#undef BLOCK_SIZE

#include "persistent-data/file_utils.h"
#include "thin-provisioning/commands.h"
#include "metadata.h"
#include "version.h"

using namespace persistent_data;
using namespace std;
using namespace thin_provisioning;

//----------------------------------------------------------------

namespace {
	void confirm_pool_is_not_active() {
		cout << "The pool must *not* be active when running this tool.\n"
		     << "Do you wish to continue? [Y/N]\n"
		     << endl;

		string input;
		cin >> input;
		if (input != "Y")
			exit(0);
	}

	class discard_emitter {
	public:
		discard_emitter(string const &data_dev, unsigned block_size, uint64_t nr_blocks)
			: fd_(open_dev(data_dev, block_size * nr_blocks)),
			  block_size_(block_size) {
		}

		~discard_emitter() {
			::close(fd_);
		}

		void emit(block_address b, block_address e) {
			uint64_t range[2];

			range[0] = block_to_byte(b);
			range[1] = block_to_byte(e) - range[0];

			if (ioctl(fd_, BLKDISCARD, &range))
				throw runtime_error("discard ioctl failed");
		}

	private:
		static int open_dev(string const &data_dev, uint64_t expected_size) {
			int r, fd;
			uint64_t blksize;
			struct stat info;

			fd = ::open(data_dev.c_str(), O_WRONLY);
			if (fd < 0) {
				ostringstream out;
				out << "Couldn't open data device '" << data_dev << "'";
				throw runtime_error(out.str());
			}

			try {
				r = fstat(fd, &info);
				if (r)
					throw runtime_error("Couldn't stat data device");

				if (!S_ISBLK(info.st_mode))
					throw runtime_error("Data device is not a block device");

				r = ioctl(fd, BLKGETSIZE64, &blksize);
				if (r)
					throw runtime_error("Couldn't get data device size");

				if (blksize != (expected_size << 9))
					throw runtime_error("Data device is not the expected size");

			} catch (...) {
				::close(fd);
				throw;
			}

			return fd;
		}

		uint64_t block_to_byte(block_address b) {
			return (b * block_size_) << 9;
		}

		int fd_;
		unsigned block_size_;
	};

	class trim_visitor : public space_map_detail::visitor {
	public:
		trim_visitor(discard_emitter &e)
		: emitter_(e) {
		}

		virtual void visit(space_map_detail::missing_counts const &mc) {
			throw std::runtime_error("corrupt metadata, please use thin_check for details");
		}

		virtual void visit(block_address b, uint32_t count) {
			if (last_visited_ && (b > *last_visited_ + 1))
				emitter_.emit(*last_visited_ + 1, b);

			last_visited_ = b;
		}

	private:
		discard_emitter &emitter_;
		boost::optional<block_address> last_visited_;
	};

	int trim(string const &metadata_dev, string const &data_dev) {
		// We can trim any block that has zero count in the data
		// space map.
		block_manager<>::ptr bm = open_bm(metadata_dev, block_manager<>::READ_ONLY);
		metadata md(bm);

		if (!md.data_sm_->get_nr_free())
			return 0;

		discard_emitter de(data_dev, md.sb_.data_block_size_,
				   md.data_sm_->get_nr_blocks());
		trim_visitor tv(de);

		confirm_pool_is_not_active();
		md.data_sm_->visit(tv);

		return 0;
	}

	struct flags {
		boost::optional<string> metadata_dev;
		boost::optional<string> data_dev;
	};
}

//----------------------------------------------------------------

thin_trim_cmd::thin_trim_cmd()
	: command("thin_trim")
{
}

void
thin_trim_cmd::usage(std::ostream &out) const
{
	out << "Usage: " << get_name() << " [options] {device|file}\n"
	    << "Options:\n"
	    << "  {--pool-inactive}\n"
	    << "  {-h|--help}\n"
	    << "  {-V|--version}" << endl;
}

int
thin_trim_cmd::run(int argc, char **argv)
{
	int c;
	flags fs;
	const char shortopts[] = "hV";

	const struct option longopts[] = {
		{ "help", no_argument, NULL, 'h' },
		{ "version", no_argument, NULL, 'V' },
		{ "metadata-dev", required_argument, NULL, 0 },
		{ "data-dev", required_argument, NULL, 1 },
		{ "pool-inactive", no_argument, NULL, 2 },
		{ NULL, no_argument, NULL, 0 }
	};

	while ((c = getopt_long(argc, argv, shortopts, longopts, NULL)) != -1) {
		switch(c) {
		case 0:
			fs.metadata_dev = optarg;
			break;

		case 1:
			fs.data_dev = optarg;
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

	if (!fs.metadata_dev || !fs.data_dev) {
		usage(cerr);
		return 1;
	}

	return trim(*fs.metadata_dev, *fs.data_dev);
}

//----------------------------------------------------------------

#include <iostream>
#include <sstream>
#include <string>
#include <stdexcept>

#include <errno.h>
#include <fcntl.h>
#include <getopt.h>
#include <libgen.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include "base/error_state.h"
#include "base/nested_output.h"
#include "era/commands.h"
#include "era/writeset_tree.h"
#include "era/era_array.h"
#include "era/superblock.h"
#include "persistent-data/block.h"
#include "persistent-data/file_utils.h"
#include "persistent-data/space_map.h"
#include "persistent-data/space-maps/core.h"
#include "persistent-data/transaction_manager.h"
#include "version.h"

using namespace base;
using namespace boost;
using namespace era;
using namespace persistent_data;
using namespace std;

//----------------------------------------------------------------

namespace {
	class reporter_base {
	public:
		reporter_base(nested_output &o)
		: out_(o),
		  err_(NO_ERROR) {
		}

		virtual ~reporter_base() {}

		nested_output &out() {
			return out_;
		}

		nested_output::nest push() {
			return out_.push();
		}

		base::error_state get_error() const {
			return err_;
		}

		void mplus_error(error_state err) {
			err_ = combine_errors(err_, err);
		}

	private:
		nested_output &out_;
		error_state err_;
	};

	class superblock_reporter : public superblock_damage::damage_visitor, reporter_base {
	public:
		superblock_reporter(nested_output &o)
		: reporter_base(o) {
		}

		virtual void visit(superblock_damage::superblock_corrupt const &d) {
			out() << "superblock is corrupt" << end_message();
			{
				nested_output::nest _ = push();
				out() << d.get_desc() << end_message();
			}

			mplus_error(FATAL);
		}

		virtual void visit(superblock_damage::superblock_invalid const &d) {
			out() << "superblock is invalid" << end_message();
			{
				nested_output::nest _ = push();
				out() << d.get_desc() << end_message();
			}

			mplus_error(FATAL);
		}

		using reporter_base::get_error;
	};

	class writeset_tree_reporter : public writeset_tree_detail::damage_visitor, reporter_base {
	public:
		writeset_tree_reporter(nested_output &o)
		: reporter_base(o) {
		}

		void visit(writeset_tree_detail::missing_eras const &d) {
			out() << "missing eras from writeset tree" << end_message();
			{
				nested_output::nest _ = push();
				out() << d.get_desc() << end_message();
				out() << "Effected eras: [" << d.eras_.begin_.get()
				      << ", " << d.eras_.end_.get() << ")" << end_message();
			}

			mplus_error(FATAL);
		}

		void visit(writeset_tree_detail::damaged_writeset const &d) {
			out() << "damaged writeset" << end_message();
			{
				nested_output::nest _ = push();
				out() << d.get_desc() << end_message();
				out() << "Era: " << d.era_ << end_message();
				out() << "Missing bits: [" << d.missing_bits_.begin_.get()
				      << ", " << d.missing_bits_.end_.get() << ")" << end_message();
			}

			mplus_error(FATAL);
		}

		using reporter_base::get_error;
	};

	class era_array_reporter : public era_array_detail::damage_visitor, reporter_base {
	public:
		era_array_reporter(nested_output &o)
		: reporter_base(o) {
		}

		void visit(era_array_detail::missing_eras const &d) {
			out() << "missing eras from era array" << end_message();
			{
				nested_output::nest _ = push();
				out() << d.get_desc() << end_message();
				out() << "Effected eras: [" << d.eras_.begin_.get()
				      << ", " << d.eras_.end_.get() << ")" << end_message();
			}

			mplus_error(FATAL);
		}

		void visit(era_array_detail::invalid_era const &d) {
			out() << "invalid era in era array" << end_message();
			{
				nested_output::nest _ = push();
				out() << d.get_desc() << end_message();
				out() << "block: " << d.block_ << ", era: " << d.era_ << end_message();
			}

			mplus_error(FATAL);
		}

		using reporter_base::get_error;
	};

	//--------------------------------

	transaction_manager::ptr open_tm(block_manager<>::ptr bm) {
		space_map::ptr sm(new core_map(bm->get_nr_blocks()));
		sm->inc(SUPERBLOCK_LOCATION);
		transaction_manager::ptr tm(new transaction_manager(bm, sm));
		return tm;
	}

	//--------------------------------

	struct flags {
		flags()
			: superblock_only_(false),
			  quiet_(false) {
		}

		bool superblock_only_;
		bool quiet_;
	};

	struct stat guarded_stat(string const &path) {
		struct stat info;

		int r = ::stat(path.c_str(), &info);
		if (r) {
			ostringstream msg;
			char buffer[128], *ptr;

			ptr = ::strerror_r(errno, buffer, sizeof(buffer));
			msg << path << ": " << ptr;
			throw runtime_error(msg.str());
		}

		return info;
	}

	error_state metadata_check(block_manager<>::ptr bm, flags const &fs) {
		nested_output out(cerr, 2);
		if (fs.quiet_)
			out.disable();

		superblock_reporter sb_rep(out);

		out << "examining superblock" << end_message();
		{
			nested_output::nest _ = out.push();
			check_superblock(bm, bm->get_nr_blocks(), sb_rep);
		}

		if (sb_rep.get_error() == FATAL)
			return FATAL;

		superblock sb = read_superblock(bm);
		transaction_manager::ptr tm = open_tm(bm);

		writeset_tree_reporter wt_rep(out);
		{
			era_detail_traits::ref_counter rc(tm);
			writeset_tree wt(*tm, sb.writeset_tree_root, rc);
			check_writeset_tree(tm, wt, wt_rep);
		}

		era_array_reporter ea_rep(out);
		{
			uint32_traits::ref_counter rc;
			era_array ea(*tm, rc, sb.era_array_root, sb.nr_blocks);
			check_era_array(ea, sb.current_era, ea_rep);
		}

		return combine_errors(sb_rep.get_error(),
				      combine_errors(wt_rep.get_error(),
						     ea_rep.get_error()));
	}

	int check(string const &path, flags const &fs) {
		error_state err;
		struct stat info = guarded_stat(path);

		if (!S_ISREG(info.st_mode) && !S_ISBLK(info.st_mode)) {
			ostringstream msg;
			msg << path << ": " << "Not a block device or regular file";
			throw runtime_error(msg.str());
		}

		block_manager<>::ptr bm = open_bm(path, block_manager<>::READ_ONLY);
		err = metadata_check(bm, fs);

		return err == NO_ERROR ? 0 : 1;
	}

	int check_with_exception_handling(string const &path, flags const &fs) {
		int r;
		try {
			r = check(path, fs);

		} catch (std::exception &e) {
			if (!fs.quiet_)
				cerr << e.what() << endl;
			r = 1;
		}

		return r;

	}

	void usage(ostream &out, string const &cmd) {
		out << "Usage: " << cmd << " [options] {device|file}" << endl
		    << "Options:" << endl
		    << "  {-q|--quiet}" << endl
		    << "  {-h|--help}" << endl
		    << "  {-V|--version}" << endl
		    << "  {--super-block-only}" << endl;
	}
}

//----------------------------------------------------------------

int era_check_main(int argc, char **argv)
{
	int c;
	flags fs;
	const char shortopts[] = "qhV";
	const struct option longopts[] = {
		{ "quiet", no_argument, NULL, 'q' },
		{ "super-block-only", no_argument, NULL, 1 },
		{ "help", no_argument, NULL, 'h' },
		{ "version", no_argument, NULL, 'V' },
		{ NULL, no_argument, NULL, 0 }
	};

	while ((c = getopt_long(argc, argv, shortopts, longopts, NULL)) != -1) {
		switch(c) {
		case 1:
			fs.superblock_only_ = true;
			break;

		case 'h':
			usage(cout, basename(argv[0]));
			return 0;

		case 'q':
			fs.quiet_ = true;
			break;

		case 'V':
			cout << THIN_PROVISIONING_TOOLS_VERSION << endl;
			return 0;

		default:
			usage(cerr, basename(argv[0]));
			return 1;
		}
	}

	if (argc == optind) {
		cerr << "No input file provided." << endl;
		usage(cerr, basename(argv[0]));
		return 1;
	}

	return check_with_exception_handling(argv[optind], fs);
}

base::command era::era_check_cmd("era_check", era_check_main);

//----------------------------------------------------------------

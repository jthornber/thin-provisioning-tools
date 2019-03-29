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

#include <boost/lexical_cast.hpp>
#include <boost/optional.hpp>
#include <getopt.h>
#include <vector>
#include <fstream>

#include "persistent-data/data-structures/btree.h"
#include "persistent-data/data-structures/simple_traits.h"
#include "persistent-data/file_utils.h"
#include "persistent-data/space-maps/core.h"
#include "persistent-data/space-maps/disk_structures.h"
#include "thin-provisioning/metadata.h"
#include "thin-provisioning/superblock.h"
#include "thin-provisioning/commands.h"
#include "version.h"

using namespace boost;
using namespace std;
using namespace thin_provisioning;

//----------------------------------------------------------------

namespace {
	bool check_flags(uint32_t flags) {
		flags &= 0x3;
		if (flags == INTERNAL_NODE || flags == LEAF_NODE)
			return true;
		return false;
	}

	// extracted from btree_damage_visitor.h
	template <typename node>
	bool check_block_nr(node const &n) {
		if (n.get_location() != n.get_block_nr()) {
			return false;
		}
		return true;
	}

	// extracted from btree_damage_visitor.h
	template <typename node>
	bool check_max_entries(node const &n) {
		size_t elt_size = sizeof(uint64_t) + n.get_value_size();
		if (elt_size * n.get_max_entries() + sizeof(node_header) > MD_BLOCK_SIZE) {
			return false;
		}

		if (n.get_max_entries() % 3) {
			return false;
		}

		return true;
	}

	// extracted from btree_damage_visitor.h
	template <typename node>
	bool check_nr_entries(node const &n, bool is_root) {
		if (n.get_nr_entries() > n.get_max_entries()) {
			return false;
		}

		block_address min = n.get_max_entries() / 3;
		if (!is_root && (n.get_nr_entries() < min)) {
			return false;
		}

		return true;
	}

	// extracted from btree_damage_visitor.h
	template <typename node>
	bool check_ordered_keys(node const &n) {
		unsigned nr_entries = n.get_nr_entries();

		if (nr_entries == 0)
			return true; // can only happen if a root node

		uint64_t last_key = n.key_at(0);

		for (unsigned i = 1; i < nr_entries; i++) {
			uint64_t k = n.key_at(i);
			if (k <= last_key) {
				return false;
			}
			last_key = k;
		}

		return true;
	}
}

namespace {
	uint32_t const SUPERBLOCK_CSUM_SEED = 160774;
	uint32_t const BITMAP_CSUM_XOR = 240779;
	uint32_t const INDEX_CSUM_XOR = 160478;
	uint32_t const BTREE_CSUM_XOR = 121107;

	enum metadata_block_type {
		UNKNOWN = 0,
		ZERO,
		SUPERBLOCK,
		INDEX_BLOCK,
		BITMAP_BLOCK,
		BTREE_NODE
	};

	// For UNKNOWN and ZERO
	class block_range {
	public:
		block_range()
			: begin_(0),
			  end_(0),
			  type_(UNKNOWN),
			  is_valid_(false),
			  ref_count_(-1) {
		}

		block_range(block_range const &rhs)
			: begin_(rhs.begin_),
			  end_(rhs.end_),
			  type_(rhs.type_),
			  is_valid_(rhs.is_valid_),
			  ref_count_(rhs.ref_count_) {
		}

		virtual ~block_range() {}

		virtual void reset(int type,
				   typename block_manager<>::read_ref &rr,
				   int64_t ref_count) {
			begin_ = rr.get_location();
			end_ = begin_ + 1;
			type_ = type;
			ref_count_ = ref_count;
			is_valid_ = false;
		}

		virtual std::unique_ptr<block_range> clone() const {
			return std::unique_ptr<block_range>(new block_range(*this));
		}

		inline uint64_t size() const {
			return (end_ > begin_) ? (end_ - begin_) : 0;
		}

		// returns true if r is left or right-adjacent
		bool is_adjacent_to(block_range const &r) const {
			if (begin_ < r.begin_)
				return is_adjacent_to_(r);
			return r.is_adjacent_to_(*this);
		}

		bool concat(block_range const &r) {
			if (!is_adjacent_to(r))
				return false;
			begin_ = std::min(begin_, r.begin_);
			end_ = std::max(end_, r.end_);
			return true;
		}

		virtual char const *type_name() const {
			switch (type_) {
			case ZERO:
				return "zero";
			default:
				return "unknown";
			}
		}

		virtual void print(std::ostream &out) const {
			uint64_t s = size();

			if (s > 1) {
				out << "<range_block type=\"" << type_name()
				    << "\" location_begin=\"" << begin_
				    << "\" length=\"" << s
				    << "\" ref_count=\"" << ref_count_
				    << "\" is_valid=\"" << is_valid_
				    << "\"/>";
			} else if (s == 1) {
				out << "<single_block type=\"" << type_name()
				    << "\" location=\"" << begin_
				    << "\" ref_count=\"" << ref_count_
				    << "\" is_valid=\"" << is_valid_
				    << "\"/>";
			}
		}

		friend ostream &operator<<(std::ostream &out, block_range const &r);

	protected:
		// return true is rhs is right-adjacent
		virtual bool is_adjacent_to_(block_range const &rhs) const {
			if (type_ != rhs.type_)
				return false;

			if (rhs.begin_ != end_)
				return false;

			if (ref_count_ != rhs.ref_count_ ||
			    is_valid_ != rhs.is_valid_)
				return false;

			return true;
		}

		uint64_t begin_;
		uint64_t end_; // one-pass-the-end. end_ == begin_ indicates an empty range.
		int type_;
		bool is_valid_;
		int64_t ref_count_; // ref_count in metadata space map
	};

	// For SUPERBLOCK, INDEX_BLOCK and BITMAP_BLOCK
	class meta_block_range: public block_range {
	public:
		meta_block_range()
			: block_range(),
			  blocknr_begin_(0) {
		}

		meta_block_range(meta_block_range const &rhs)
			: block_range(rhs),
			  blocknr_begin_(rhs.blocknr_begin_) {
		}

		virtual void reset(int type,
				   typename block_manager<>::read_ref &rr,
				   int64_t ref_count) {
			using namespace persistent_data;
			using namespace sm_disk_detail;
			using namespace superblock_detail;

			begin_ = rr.get_location();
			end_ = begin_ + 1;
			type_ = type;
			ref_count_ = ref_count;

			switch (type) {
			case SUPERBLOCK:
				blocknr_begin_ = to_cpu<uint64_t>(reinterpret_cast<superblock_disk const *>(rr.data())->blocknr_);
				break;
			case BITMAP_BLOCK:
				blocknr_begin_ = to_cpu<uint64_t>(reinterpret_cast<bitmap_header const *>(rr.data())->blocknr);
				break;
			case INDEX_BLOCK:
				blocknr_begin_ = to_cpu<uint64_t>(reinterpret_cast<metadata_index const *>(rr.data())->blocknr_);
				break;
			default:
				blocknr_begin_ = 0;
			}

			is_valid_ = (blocknr_begin_ == begin_) ? true : false;
		}

		virtual std::unique_ptr<block_range> clone() const {
			return std::unique_ptr<block_range>(new meta_block_range(*this));
		}

		virtual char const *type_name() const {
			switch (type_) {
			case SUPERBLOCK:
				return "superblock";
			case INDEX_BLOCK:
				return "index_block";
			case BITMAP_BLOCK:
				return "bitmap_block";
			default:
				return "unknown";
			}
		}

		virtual void print(std::ostream &out) const {
			uint64_t s = size();

			if (s > 1) {
				out << "<range_block type=\"" << type_name()
				    << "\" location_begin=\"" << begin_
				    << "\" blocknr_begin=\"" << blocknr_begin_
				    << "\" length=\"" << s
				    << "\" ref_count=\"" << ref_count_
				    << "\" is_valid=\"" << is_valid_
				    << "\"/>";
			} else if (s == 1) {
				out << "<single_block type=\"" << type_name()
				    << "\" location=\"" << begin_
				    << "\" blocknr=\"" << blocknr_begin_
				    << "\" ref_count=\"" << ref_count_
				    << "\" is_valid=\"" << is_valid_
				    << "\"/>";
			}
		}

	protected:
		virtual bool is_adjacent_to_(block_range const &rhs) const {
			if (!block_range::is_adjacent_to_(rhs))
				return false;
			meta_block_range const &r = dynamic_cast<meta_block_range const &>(rhs);
			if (r.blocknr_begin_ < blocknr_begin_)
				return false;
			if (r.blocknr_begin_ - blocknr_begin_ != r.begin_ - begin_)
				return false;
			return true;
		}

		block_address blocknr_begin_; // block number in header
	};

	// For BTREE_NODE
	class btree_block_range: public meta_block_range {
	public:
		btree_block_range()
			: meta_block_range(),
			  flags_(0),
			  value_size_(0) {
		}

		btree_block_range(btree_block_range const &rhs)
			: meta_block_range(rhs),
			  flags_(rhs.flags_),
			  value_size_(rhs.value_size_) {
		}

		virtual void reset(int type,
				   typename block_manager<>::read_ref &rr,
				   int64_t ref_count) {
			node_ref<uint64_traits> n = btree_detail::to_node<uint64_traits>(rr);

			begin_ = rr.get_location();
			end_ = begin_ + 1;
			type_ = type;
			ref_count_ = ref_count;
			blocknr_begin_ = n.get_block_nr();
			flags_ = to_cpu<uint32_t>(n.raw()->header.flags);
			value_size_ = n.get_value_size();

			if (check_flags(flags_) &&
			    check_block_nr(n) &&
			    check_max_entries(n) &&
			    check_nr_entries(n, true) &&
			    check_ordered_keys(n))
				is_valid_ = true;
			else
				is_valid_ = false;
		}

		virtual std::unique_ptr<block_range> clone() const {
			return std::unique_ptr<block_range>(new btree_block_range(*this));
		}

		virtual char const *type_name() const {
			if ((flags_ & INTERNAL_NODE) && !(flags_ & LEAF_NODE))
				return "btree_internal";
			else if (flags_ & LEAF_NODE)
				return "btree_leaf";
			else
				return "btree_unknown";
		};

		virtual void print(std::ostream &out) const {
			uint64_t s = size();

			if (s > 1) {
				out << "<range_block type=\"" << type_name()
				    << "\" location_begin=\"" << begin_
				    << "\" blocknr_begin=\"" << blocknr_begin_
				    << "\" length=\"" << s
				    << "\" ref_count=\"" << ref_count_
				    << "\" is_valid=\"" << is_valid_
				    << "\" value_size=\"" << value_size_
				    << "\"/>";
			} else if (s == 1) {
				out << "<single_block type=\"" << type_name()
				    << "\" location=\"" << begin_
				    << "\" blocknr=\"" << blocknr_begin_
				    << "\" ref_count=\"" << ref_count_
				    << "\" is_valid=\"" << is_valid_
				    << "\" value_size=\"" << value_size_
				    << "\"/>";
			}
		}

	protected:
		virtual bool is_adjacent_to_(block_range const &rhs) const {
			if (!meta_block_range::is_adjacent_to_(rhs))
				return false;
			btree_block_range const &r = dynamic_cast<btree_block_range const &>(rhs);
			if ((flags_ & 0x3) != (r.flags_ & 0x3))
				return false;
			if (value_size_ != r.value_size_)
				return false;
			return true;
		}

		uint32_t flags_;
		size_t value_size_;
	};

	ostream &operator<<(std::ostream &out, block_range const &r) {
		r.print(out);
		return out;
	}

	//-------------------------------------------------------------------

	class range_factory {
	public:
		virtual ~range_factory() {}

		block_range const &convert_to_range(block_manager<>::read_ref rr, int64_t ref_count) {
			if (!memcmp(rr.data(), zeros_.data(), MD_BLOCK_SIZE)) {
				br_.reset(ZERO, rr, ref_count);
				return br_;
			}

			uint32_t const *cksum = reinterpret_cast<uint32_t const*>(rr.data());
			base::crc32c sum(*cksum);
			sum.append(cksum + 1, MD_BLOCK_SIZE - sizeof(uint32_t));

			switch (sum.get_sum()) {
			case SUPERBLOCK_CSUM_SEED:
				mbr_.reset(SUPERBLOCK, rr, ref_count);
				return mbr_;
			case INDEX_CSUM_XOR:
				mbr_.reset(INDEX_BLOCK, rr, ref_count);
				return mbr_;
			case BITMAP_CSUM_XOR:
				mbr_.reset(BITMAP_BLOCK, rr, ref_count);
				return mbr_;
			case BTREE_CSUM_XOR:
				bbr_.reset(BTREE_NODE, rr, ref_count);
				return bbr_;
			default:
				br_.reset(UNKNOWN, rr, ref_count);
				return br_;
			}
		}

	private:
		static const std::vector<char> zeros_;

		// for internal caching only
		block_range br_;
		meta_block_range mbr_;
		btree_block_range bbr_;
	};

	const std::vector<char> range_factory::zeros_(MD_BLOCK_SIZE, 0);

	class metadata_scanner {
	public:
		metadata_scanner(block_manager<>::ptr bm, uint64_t scan_begin, uint64_t scan_end,
                                 bool check_for_strings)
			: bm_(bm),
			  scan_begin_(scan_begin),
			  scan_end_(scan_end),
			  index_(scan_begin),
			  check_for_strings_(check_for_strings) {
			if (scan_end_ <= scan_begin_)
				throw std::runtime_error("badly formed region (end <= begin)");

			// try to open metadata space-map (it's okay to fail)
			try {
				superblock_detail::superblock sb = read_superblock(bm);
				tm_ = open_tm(bm, superblock_detail::SUPERBLOCK_LOCATION);
				metadata_sm_ = open_metadata_sm(*tm_, &sb.metadata_space_map_root_);
				tm_->set_sm(metadata_sm_);
			} catch (std::exception &e) {
				cerr << e.what() << endl;
			}

			// prefetch the first block
			block_range const &r = read_block(index_++);
			run_range_ = r.clone();
		}

		std::unique_ptr<block_range> get_range() {
			std::unique_ptr<block_range> ret;

			while (index_ < scan_end_) {
				block_range const &r = read_block(index_++);

				if (!run_range_->concat(r)) {
					ret = std::move(run_range_);
					run_range_ = r.clone();
					break;
				}
			}
			if (!ret) { // for the last run (index_ == scan_end_)
				ret = std::move(run_range_);
				run_range_.reset();
			}
			return ret;
		}

		map<block_address, vector<string>> const &get_strings() const {
			return strings_;
		}

	private:
		bool interesting_char(char c)
		{
			return isalnum(c) || ispunct(c);
		}

		unsigned printable_len(const char *b, const char *e)
		{
			const char *p = b;

			while (p != e && interesting_char(*p))
				p++;

			return p - b;
		}

		// asci text within our metadata is a sure sign of corruption.
		optional<vector<string> >
		scan_strings(block_manager<>::read_ref rr)
		{
			vector<string> r;
			const char *data = reinterpret_cast<const char *>(rr.data()), *end = data + MD_BLOCK_SIZE;

			while (data < end) {
				auto len = printable_len(data, end);
				if (len >= 4)
					r.push_back(string(data, data + len));

				data += len + 1;
			}

			return r.size() ? optional<vector<string>>(r) : optional<vector<string>>();
		}

		block_range const &read_block(block_address b) {
			block_manager<>::read_ref rr = bm_->read_lock(b);
			int64_t ref_count;
			try {
				ref_count = metadata_sm_ ? static_cast<int64_t>(metadata_sm_->get_count(b)) : -1;
			} catch (std::exception &e) {
				ref_count = -1;
			}

			if (check_for_strings_) {
				auto ss = scan_strings(rr);
				if (ss) {
					strings_.insert(make_pair(b, *ss));
				}
			}

			return factory_.convert_to_range(rr, ref_count);
		}

		// note: space_map does not take the ownership of transaction_manager,
		// so the transaction_manager must live in the same scope of space_map.
		block_manager<>::ptr bm_;
		transaction_manager::ptr tm_;
		checked_space_map::ptr metadata_sm_;

		uint64_t scan_begin_;
		uint64_t scan_end_;
		uint64_t index_;
		std::unique_ptr<block_range> run_range_;

		range_factory factory_;

		bool check_for_strings_;
		map<block_address, vector<string>> strings_;
	};

	//-------------------------------------------------------------------

	struct flags {
		flags()
		: exclusive_(true),
		  examine_corruption_(false)
		{
		}

		boost::optional<block_address> scan_begin_;
		boost::optional<block_address> scan_end_;
		bool exclusive_;
		bool examine_corruption_;
	};

	int scan_metadata_(string const &input,
			   std::ostream &out,
			   flags const &f) {
		block_manager<>::ptr bm;
		bm = open_bm(input, block_manager<>::READ_ONLY, f.exclusive_);
		block_address scan_begin = f.scan_begin_ ? *f.scan_begin_ : 0;
		block_address scan_end = f.scan_end_ ? *f.scan_end_ : bm->get_nr_blocks();

		metadata_scanner scanner(bm, scan_begin, scan_end, f.examine_corruption_);
		std::unique_ptr<block_range> r;
		while ((r = scanner.get_range())) {
			out << *r << std::endl;
		}

		if (f.examine_corruption_) {
			auto ss = scanner.get_strings();

			for (auto const &ps : ss) {
				out << ps.first << ": ";

				unsigned total = 0;
				for (auto const &s : ps.second)
					total += s.length();

				out << total << " bytes of text\n";
			}
		}

		return 0;
	}

	int scan_metadata(string const &input,
			  boost::optional<string> output,
			  flags const &f) {
		try {
			if (output) {
				std::ofstream out(output->c_str());
				scan_metadata_(input, out, f);
			} else
				scan_metadata_(input, cout, f);
		} catch (std::exception &e) {
			cerr << e.what() << endl;
			return 1;
		}
		return 0;
	}
}

//---------------------------------------------------------------------------

thin_scan_cmd::thin_scan_cmd()
	: command("thin_scan")
{
}

void
thin_scan_cmd::usage(std::ostream &out) const {
	out << "Usage: " << get_name() << " [options] {device|file}\n"
	    << "Options:\n"
	    << "  {-h|--help}\n"
	    << "  {-o|--output} <xml file>\n"
	    << "  {--begin} <block#>\n"
	    << "  {--end} <block#>\n"
	    << "  {--examine-corruption}\n"
	    << "  {-V|--version}" << endl;
}

int
thin_scan_cmd::run(int argc, char **argv)
{
	const char shortopts[] = "ho:V";
	const struct option longopts[] = {
		{ "help", no_argument, NULL, 'h'},
		{ "output", required_argument, NULL, 'o'},
		{ "version", no_argument, NULL, 'V'},
		{ "begin", required_argument, NULL, 1},
		{ "end", required_argument, NULL, 2},
		{ "examine-corruption", no_argument, NULL, 3 },
		{ NULL, no_argument, NULL, 0 }
	};
	boost::optional<string> output;
	flags f;

	int c;
	while ((c = getopt_long(argc, argv, shortopts, longopts, NULL)) != -1) {
		switch(c) {
		case 'h':
			usage(cout);
			return 0;

		case 'o':
			output = optarg;
			break;

		case 'V':
			cout << THIN_PROVISIONING_TOOLS_VERSION << endl;
			return 0;

		case 1:
			try {
				f.scan_begin_ = boost::lexical_cast<uint64_t>(optarg);
			} catch (std::exception &e) {
				cerr << e.what() << endl;
				return 1;
			}
			break;

		case 2:
			try {
				f.scan_end_ = boost::lexical_cast<uint64_t>(optarg);
			} catch (std::exception &e) {
				cerr << e.what() << endl;
				return 1;
			}
			break;

		case 3:
			f.examine_corruption_ = true;
			break;

		default:
			usage(cerr);
			return 1;
		}
	}

	if (argc == optind) {
		cerr << "No input file provided." << endl;
		usage(cerr);
		return 1;
	}

	if (f.scan_begin_ && f.scan_end_ && (*f.scan_end_ <= *f.scan_begin_)) {
		cerr << "badly formed region (end <= begin)" << endl;
		return 1;
	}

	return scan_metadata(argv[optind], output, f);
}

//---------------------------------------------------------------------------

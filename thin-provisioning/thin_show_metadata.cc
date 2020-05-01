#include "persistent-data/checksum.h"
#include "persistent-data/data-structures/btree.h"
#include "persistent-data/file_utils.h"
#include "persistent-data/space-maps/disk_structures.h"
#include "thin-provisioning/commands.h"
#include "thin-provisioning/metadata.h"
#include "thin-provisioning/superblock.h"
#include "ui/ui.h"
#include "version.h"

#include <iostream>
#include <getopt.h>
#include <libgen.h>
#include <stdlib.h>
#include <stdexcept>
#include <memory>

using namespace persistent_data;
using namespace sm_disk_detail;
using namespace std;
using namespace thin_provisioning;
using namespace ui;

//----------------------------------------------------------------

namespace {
	class examiner {
	public:
		examiner(string const &name, int colour_pair, char rep)
			: name_(name),
			  colour_pair_(colour_pair),
			  rep_(rep) {
		}

		virtual ~examiner() {}

		virtual bool recognise(block_manager::read_ref rr) const = 0;
//		virtual void render_block(text_ui &ui, block_manager::read_ref rr) = 0;

		string const &get_name() const {
			return name_;
		}

		int get_color_pair() const {
			return colour_pair_;
		}

		char get_rep() const {
			return rep_;
		}

	private:
		string name_;
		int colour_pair_;
		char rep_;
	};

	class raw_examiner : public examiner {
	public:
		raw_examiner()
			: examiner("raw", 5, '?') {
		}

		virtual bool recognise(block_manager::read_ref rr) const {
			return true;
		}
	};

	class superblock_examiner : public examiner {
	public:
		superblock_examiner()
			: examiner("superblock", 1, 'S') {
		}

		virtual bool recognise(block_manager::read_ref rr) const {
			using namespace superblock_detail;

			superblock_disk const *sbd = reinterpret_cast<superblock_disk const *>(rr.data());
			if (to_cpu<uint64_t>(sbd->magic_) == SUPERBLOCK_MAGIC) {
				superblock sb;
				superblock_traits::unpack(*sbd, sb);
				cout << "metadata nr blocks: " << sb.metadata_nr_blocks_ << endl;

				return true;
			}

			return false;
		}
	};

	class bitmap_examiner : public examiner {
	public:
		bitmap_examiner()
			: examiner("bitmap", 2, ':') {
		}

		virtual bool recognise(block_manager::read_ref rr) const {
			bitmap_header const *data = reinterpret_cast<bitmap_header const *>(rr.data());
			crc32c sum(BITMAP_CSUM_XOR);
			sum.append(&data->not_used, MD_BLOCK_SIZE - sizeof(uint32_t));
			return sum.get_sum() == to_cpu<uint32_t>(data->csum);
		}
	};

	class index_examiner : public examiner {
	public:
		index_examiner()
			: examiner("index", 3, 'i') {
		}

		virtual bool recognise(block_manager::read_ref rr) const {
			metadata_index const *mi = reinterpret_cast<metadata_index const *>(rr.data());
			crc32c sum(INDEX_CSUM_XOR);
			sum.append(&mi->padding_, MD_BLOCK_SIZE - sizeof(uint32_t));
			return sum.get_sum() == to_cpu<uint32_t>(mi->csum_);
		}
	};


	class btree_examiner : public examiner {
	public:
		btree_examiner(string const &name, int colour_pair, char c)
			: examiner(name, colour_pair, c) {
		}

		bool is_btree_node(block_manager::read_ref rr) const {
			using namespace btree_detail;

			disk_node const *data = reinterpret_cast<disk_node const *>(rr.data());
			node_header const *n = &data->header;
			crc32c sum(BTREE_CSUM_XOR);
			sum.append(&n->flags, MD_BLOCK_SIZE - sizeof(uint32_t));
			return sum.get_sum() == to_cpu<uint32_t>(n->csum);
		}
	};

	class dev_detail_examiner : public btree_examiner {
	public:
		dev_detail_examiner()
			: btree_examiner("dev_details", 4, 'd') {
		}

		virtual bool recognise(block_manager::read_ref rr) const {
			if (!btree_examiner::is_btree_node(rr))
				return false;

			using namespace btree_detail;

			disk_node const *data = reinterpret_cast<disk_node const *>(rr.data());
			node_header const *n = &data->header;
			return to_cpu<uint32_t>(n->value_size) == sizeof(device_tree_detail::device_details_disk);
		}
	};

	class ref_count_examiner : public btree_examiner {
	public:
		ref_count_examiner()
			: btree_examiner("ref_count node", 6, 'r') {
		}

		virtual bool recognise(block_manager::read_ref rr) const {
			if (!btree_examiner::is_btree_node(rr))
				return false;

			using namespace btree_detail;

			disk_node const *data = reinterpret_cast<disk_node const *>(rr.data());
			node_header const *n = &data->header;
			return to_cpu<uint32_t>(n->value_size) == sizeof(uint32_t);
		}
	};

	class mapping_examiner : public btree_examiner {
	public:
		mapping_examiner()
			: btree_examiner("mapping node", 7, 'm') {
		}

		virtual bool recognise(block_manager::read_ref rr) const {
			if (!btree_examiner::is_btree_node(rr))
				return false;

			using namespace btree_detail;

			disk_node const *data = reinterpret_cast<disk_node const *>(rr.data());
			node_header const *n = &data->header;
			return to_cpu<uint32_t>(n->value_size) == sizeof(uint64_t);
		}
	};

	class main_dialog {
	public:
		main_dialog(text_ui &ui,
			    block_manager const &bm)
			: ui_(ui),
			  bm_(bm),
			  raw_examiner_(new raw_examiner()) {

			examiners_.push_back(shared_ptr<examiner>(new superblock_examiner()));
			examiners_.push_back(shared_ptr<examiner>(new bitmap_examiner()));
			examiners_.push_back(shared_ptr<examiner>(new index_examiner()));
			examiners_.push_back(shared_ptr<examiner>(new dev_detail_examiner()));
			examiners_.push_back(shared_ptr<examiner>(new ref_count_examiner()));
			examiners_.push_back(shared_ptr<examiner>(new mapping_examiner()));
		}

		void run() {
			auto line_length = 80;
			for (block_address b = 0; b < 2000; b++) {
				block_manager::read_ref rr = bm_.read_lock(b);

				if (!(b % line_length)) {
					if (b > 0)
						printw("\n");

					printw("%8llu: ", b);
				}

				auto e = find_examiner(rr);
				attron(COLOR_PAIR(e->get_color_pair()));
				printw("%c", e->get_rep());
				attroff(COLOR_PAIR(e->get_color_pair()));
			}

			printw("\n");
			show_superblock();
		}

	private:
		void show_superblock() {
			auto sb = read_superblock(bm_);

			printw("\n\nSuperblock at 0\n");
			printw("data mapping root: %llu\n", sb.data_mapping_root_);
			printw("device details root: %llu\n", sb.device_details_root_);
			printw("data block size: %u\n", sb.data_block_size_);
			printw("metadata nr blocks: %llu\n", sb.metadata_nr_blocks_);
		}

		shared_ptr<examiner> &find_examiner(block_manager::read_ref const &rr) {
			for (shared_ptr<examiner> &e : examiners_) {
				if (e->recognise(rr))
					return e;
			}

			return raw_examiner_;
		}

		text_ui &ui_;
		block_manager const &bm_;
		list<shared_ptr<examiner>> examiners_;
		shared_ptr<examiner> raw_examiner_;


#if 0
		void show_superblock(text_ui &ui, superblock_detail::superblock const &sb) {
		}

		void show_blocks(text_ui &ui, string const &dev) {
			metadata md(bm);

			show_superblock(ui, md.sb_);

#if 0
			cout << "Metadata space map: nr_blocks = " << md.metadata_sm_->get_nr_blocks()
			     << ", nr_free_blocks = " << md.metadata_sm_->get_nr_free()
			     << endl;
			cout << "Data space map: nr_blocks = " << md.data_sm_->get_nr_blocks()
			     << ", nr_free_blocks = " << md.data_sm_->get_nr_free()
			     << endl;

			block_address nr_blocks = bm->get_nr_blocks();
			for (block_address b = 0; b < nr_blocks; b++) {
				block_manager::read_ref rr = bm->read_lock(b);

				if (is_superblock(rr))
					cout << b << ": superblock" << endl;

				else if (is_bitmap_block(rr))
					cout << b << ": bitmap block" << endl;

				else if (is_btree_node(rr))
					cout << b << ": btree_node" << endl;

				else
					cout << b << ": unknown" << endl;
			}
#endif
		}
#endif
	};
}

//----------------------------------------------------------------

thin_show_metadata_cmd::thin_show_metadata_cmd()
	: command("thin_show_metadata")
{
}

void
thin_show_metadata_cmd::usage(ostream &out) const
{
	out << "Usage: " << get_name() << " {device|file}" << endl
	    << "Options:" << endl
	    << "  {-h|--help}" << endl
	    << "  {-V|--version}" << endl;
}

int
thin_show_metadata_cmd::run(int argc, char **argv)
{
	int c;
	const char shortopts[] = "hV";
	const struct option longopts[] = {
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
			cerr << THIN_PROVISIONING_TOOLS_VERSION << endl;
			return 0;
		}
	}

	if (argc == optind) {
		usage(cerr);
		exit(1);
	}

	try {
		ui::text_ui ui;

		block_manager::ptr bm = open_bm(argv[optind], block_manager::READ_ONLY, true);
		main_dialog dialog(ui, *bm);
		dialog.run();
#if 0
//		show_blocks(ui, argv[optind]);
#endif


#if 0
		attron(COLOR_PAIR(1));
		printw("Hello, ");
		attron(A_BOLD);
		printw("world!\n");
		attroff(A_BOLD);
		attroff(COLOR_PAIR(1));
#endif
		getch();

	} catch (std::exception const &e) {
		cerr << e.what() << endl;
		return 1;
	}

	return 0;
}

//----------------------------------------------------------------

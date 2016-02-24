#include "persistent-data/checksum.h"
#include "persistent-data/data-structures/btree.h"
#include "persistent-data/file_utils.h"
#include "persistent-data/space-maps/disk_structures.h"
#include "thin-provisioning/commands.h"
#include "thin-provisioning/metadata.h"
#include "thin-provisioning/superblock.h"
#include "version.h"

#include <iostream>
#include <getopt.h>
#include <libgen.h>
#include <stdlib.h>
#include <stdexcept>

using namespace persistent_data;
using namespace sm_disk_detail;
using namespace std;
using namespace thin_provisioning;

//----------------------------------------------------------------

namespace {
	bool is_superblock(block_manager<>::read_ref &rr) {
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

	bool is_bitmap_block(block_manager<>::read_ref &rr) {
		bitmap_header const *data = reinterpret_cast<bitmap_header const *>(rr.data());
		crc32c sum(BITMAP_CSUM_XOR);
		sum.append(&data->not_used, MD_BLOCK_SIZE - sizeof(uint32_t));
		return sum.get_sum() == to_cpu<uint32_t>(data->csum);
	}

	bool is_index_block(block_manager<>::read_ref &rr) {
		metadata_index const *mi = reinterpret_cast<metadata_index const *>(rr.data());
		crc32c sum(INDEX_CSUM_XOR);
		sum.append(&mi->padding_, MD_BLOCK_SIZE - sizeof(uint32_t));
		return sum.get_sum() == to_cpu<uint32_t>(mi->csum_);
	}

	bool is_btree_node(block_manager<>::read_ref &rr) {
		using namespace btree_detail;

		disk_node const *data = reinterpret_cast<disk_node const *>(rr.data());
		node_header const *n = &data->header;
		crc32c sum(BTREE_CSUM_XOR);
		sum.append(&n->flags, MD_BLOCK_SIZE - sizeof(uint32_t));
		return sum.get_sum() == to_cpu<uint32_t>(n->csum);
	}

	void show_blocks(string const &dev) {
		block_manager<>::ptr bm = open_bm(dev, block_manager<>::READ_ONLY);

		metadata md(bm, metadata::OPEN);
		cout << "Metadata space map: nr_blocks = " << md.metadata_sm_->get_nr_blocks()
		     << ", nr_free_blocks = " << md.metadata_sm_->get_nr_free()
		     << endl;
		cout << "Data space map: nr_blocks = " << md.data_sm_->get_nr_blocks()
		     << ", nr_free_blocks = " << md.data_sm_->get_nr_free()
		     << endl;

		block_address nr_blocks = bm->get_nr_blocks();
		for (block_address b = 0; b < nr_blocks; b++) {
			block_manager<>::read_ref rr = bm->read_lock(b);

			if (is_superblock(rr))
				cout << b << ": superblock" << endl;

			else if (is_bitmap_block(rr))
				cout << b << ": bitmap block" << endl;

			else if (is_btree_node(rr))
				cout << b << ": btree_node" << endl;

			else
				cout << b << ": unknown" << endl;
		}
	}
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
		show_blocks(argv[optind]);

	} catch (std::exception const &e) {
		cerr << e.what() << endl;
		return 1;
	}

	return 0;
}

//----------------------------------------------------------------

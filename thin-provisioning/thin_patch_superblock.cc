#include <getopt.h>

#include "persistent-data/file_utils.h"
#include "persistent-data/space-maps/disk_structures.h"
#include "thin-provisioning/commands.h"
#include "thin-provisioning/metadata.h"
#include "thin-provisioning/superblock.h"
#include "version.h"

using namespace persistent_data;
using namespace sm_disk_detail;
using namespace thin_provisioning;

//----------------------------------------------------------------

namespace {
	struct flags {
		flags();
		bool check_conformance();

		string output;

		boost::optional<uint64_t> transaction_id;
		block_address data_mapping_root;
		block_address device_details_root;

		block_address metadata_bitmap_root;
		block_address metadata_ref_count_root;

		block_address data_bitmap_root;
		block_address data_ref_count_root;
	};

	flags::flags()
		: data_mapping_root(0), device_details_root(0),
		  metadata_bitmap_root(0), metadata_ref_count_root(0),
		  data_bitmap_root(0), data_ref_count_root(0) {
	}

	bool flags::check_conformance() {
		if (!output.size())
			return false;

		return true;
	}

	void patch_space_map_root(void *root,
				  block_address bitmap_root,
				  block_address ref_count_root) {
		sm_disk_detail::sm_root v;
		sm_disk_detail::sm_root_disk d;

		memcpy(&d, root, sizeof(d));
		sm_root_traits::unpack(d, v);

		if (bitmap_root)
			v.bitmap_root_ = bitmap_root;
		if (ref_count_root)
			v.ref_count_root_ = ref_count_root;

		sm_root_traits::pack(v, d);
		memcpy(root, &d, sizeof(d));
	}

	int patch_superblock(flags const &fs) {
		block_manager::ptr bm = open_bm(fs.output, block_manager::READ_WRITE);
		superblock_detail::superblock sb = read_superblock(bm, superblock_detail::SUPERBLOCK_LOCATION);

		if (fs.transaction_id)
			sb.trans_id_ = *fs.transaction_id;

		if (fs.data_mapping_root)
			sb.data_mapping_root_ = fs.data_mapping_root;

		if (fs.device_details_root)
			sb.device_details_root_ = fs.device_details_root;

		patch_space_map_root(sb.metadata_space_map_root_,
				     fs.metadata_bitmap_root,
				     fs.metadata_ref_count_root);

		patch_space_map_root(sb.data_space_map_root_,
				     fs.data_bitmap_root,
				     fs.data_ref_count_root);

		write_superblock(bm, sb);

		return 0;
	}
}

//----------------------------------------------------------------

thin_patch_superblock_cmd::thin_patch_superblock_cmd()
	: command("thin_patch_superblock")
{
}

void
thin_patch_superblock_cmd::usage(std::ostream &out) const
{
	out << "Usage: " << get_name() << " [options]\n"
	    << "Options:\n"
	    << "  {-h|--help}\n"
	    << "  {-o|--output} <output device or file>\n"
	    << "  {--transaction-id} <tid>\n"
	    << "  {--data-mapping-root} <blocknr>\n"
	    << "  {--device-details-root} <blocknr>\n"
	    << "  {--metadata-bitmap-root} <blocknr>\n"
	    << "  {--metadata-ref-count-root} <blocknr>\n"
	    << "  {--data-bitmap-root} <blocknr>\n"
	    << "  {--data-ref-count-root} <blocknr>\n"
	    << "  {-V|--version}" << endl;
}

int
thin_patch_superblock_cmd::run(int argc, char **argv)
{
	int c;
	flags fs;
	const char shortopts[] = "ho:V";

	const struct option longopts[] = {
		{ "help", no_argument, NULL, 'h'},
		{ "output", required_argument, NULL, 'o'},
		{ "transaction-id", required_argument, NULL, 1 },
		{ "data-mapping-root", required_argument, NULL, 2 },
		{ "device-details-root", required_argument, NULL, 3 },
		{ "metadata-bitmap-root", required_argument, NULL, 4 },
		{ "metadata-ref-count-root", required_argument, NULL, 5 },
		{ "data-bitmap-root", required_argument, NULL, 6 },
		{ "data-ref-count-root", required_argument, NULL, 7 },
		{ "version", no_argument, NULL, 'V'},
		{ NULL, no_argument, NULL, 0 }
	};

	while ((c = getopt_long(argc, argv, shortopts, longopts, NULL)) != -1) {
		switch(c) {
		case 'h':
			usage(cout);
			return 0;

		case 'o':
			fs.output = optarg;
			break;

		case 1:
			fs.transaction_id = parse_uint64(optarg, "transaction_id");
			break;

		case 2:
			fs.data_mapping_root = parse_uint64(optarg, "data_mapping_root");
			break;

		case 3:
			fs.device_details_root = parse_uint64(optarg, "device_details_root");
			break;

		case 4:
			fs.metadata_bitmap_root = parse_uint64(optarg, "metadata_bitmap_root");
			break;

		case 5:
			fs.metadata_ref_count_root = parse_uint64(optarg, "metadata_ref_count_root");
			break;

		case 6:
			fs.data_bitmap_root = parse_uint64(optarg, "data_bitmap_root");
			break;

		case 7:
			fs.data_ref_count_root = parse_uint64(optarg, "data_ref_count_root");
			break;

		case 'V':
			cout << THIN_PROVISIONING_TOOLS_VERSION << endl;
			return 0;

		default:
			usage(cerr);
			return 1;
		}
	}

	if (!fs.check_conformance()) {
		usage(cerr);
		return 1;
	}

	return patch_superblock(fs);
}

//----------------------------------------------------------------

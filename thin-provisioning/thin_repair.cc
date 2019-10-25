#include <iostream>
#include <getopt.h>
#include <libgen.h>

#include "base/file_utils.h"
#include "base/output_file_requirements.h"
#include "persistent-data/file_utils.h"
#include "thin-provisioning/commands.h"
#include "human_readable_format.h"
#include "metadata_dumper.h"
#include "metadata.h"
#include "restore_emitter.h"
#include "version.h"

using namespace persistent_data;
using namespace std;
using namespace thin_provisioning;

namespace {
        int repair(string const &old_path, string const &new_path, override_options const &opts) {
                bool metadata_touched = false;
                try {
                        // block size gets updated by the restorer
                        block_manager<>::ptr new_bm = open_bm(new_path, block_manager<>::READ_WRITE);
                        file_utils::check_file_exists(old_path, false);
                        metadata_touched = true;
                        metadata::ptr new_md(new metadata(new_bm, metadata::CREATE, 128, 0));
                        emitter::ptr inner = create_restore_emitter(new_md);
                        emitter::ptr e = create_override_emitter(inner, opts);
                        block_manager<>::ptr old_bm = open_bm(old_path, block_manager<>::READ_ONLY);
                        metadata_repair(old_bm, e, opts);

                } catch (std::exception &e) {
                        if (metadata_touched)
                                file_utils::zero_superblock(new_path);
                        cerr << e.what() << endl;
                        return 1;
                }

                return 0;
        }
}

//----------------------------------------------------------------

thin_repair_cmd::thin_repair_cmd()
	: command("thin_repair")
{
}

void
thin_repair_cmd::usage(std::ostream &out) const
{
        out << "Usage: " << get_name() << " [options] {device|file}" << endl
            << "Options:" << endl
            << "  {-h|--help}" << endl
            << "  {-i|--input} <input metadata (binary format)>" << endl
            << "  {-o|--output} <output metadata (binary format)>" << endl
            << "  {--transaction-id} <natural>" << endl
            << "  {--data-block-size} <natural>" << endl
            << "  {--nr-data-blocks} <natural>" << endl
            << "  {-V|--version}" << endl;
}

int
thin_repair_cmd::run(int argc, char **argv)
{
	int c;
	boost::optional<string> input_path, output_path;
        override_options opts;

	const char shortopts[] = "hi:o:V";

        const struct option longopts[] = {
                { "help", no_argument, NULL, 'h'},
                { "input", required_argument, NULL, 'i'},
                { "output", required_argument, NULL, 'o'},
                { "transaction-id", required_argument, NULL, 1},
                { "data-block-size", required_argument, NULL, 2},
                { "nr-data-blocks", required_argument, NULL, 3},
                { "version", no_argument, NULL, 'V'},
                { NULL, no_argument, NULL, 0 }
        };

        while ((c = getopt_long(argc, argv, shortopts, longopts, NULL)) != -1) {
                switch(c) {
                case 'h':
                        usage(cout);
                        return 0;

                case 'i':
                        input_path = optarg;
                        break;

                case 'o':
                        output_path = optarg;
                        break;

                case 1:
                        opts.transaction_id_ = parse_uint64(optarg, "transaction id");
                        break;

                case 2:
                        opts.data_block_size_ = static_cast<uint32_t>(parse_uint64(optarg, "data block size"));
                        break;

                case 3:
                        opts.nr_data_blocks_ = parse_uint64(optarg, "nr data blocks");
                        break;

                case 'V':
                        cout << THIN_PROVISIONING_TOOLS_VERSION << endl;
                        return 0;

                default:
                        usage(cerr);
                        return 1;
                }
        }

	if (!input_path) {
		cerr << "no input file provided" << endl;
		usage(cerr);
		return 1;
	}

	if (output_path)
		check_output_file_requirements(*output_path);

	else {
		cerr << "No output file provided." << endl;
		usage(cerr);
		return 1;
	}

        return repair(*input_path, *output_path, opts);

}

//----------------------------------------------------------------

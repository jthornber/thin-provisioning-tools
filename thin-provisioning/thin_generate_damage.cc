#include "base/output_file_requirements.h"
#include "persistent-data/file_utils.h"
#include "thin-provisioning/commands.h"
#include "thin-provisioning/damage_generator.h"
#include "version.h"

#include <getopt.h>
#include <unistd.h>

using namespace persistent_data;
using namespace thin_provisioning;

//----------------------------------------------------------------

namespace {
	struct flags {
		enum damage_operations {
			DAMAGE_OP_NONE,
			DAMAGE_OP_CREATE_METADATA_LEAKS,
			DAMAGE_OP_LAST
		};

		flags()
			: op(DAMAGE_OP_NONE),
			  nr_blocks(0),
			  expected_rc(0),
			  actual_rc(0) {
		}

		bool check_conformance();

		damage_operations op;
		string output;
		block_address nr_blocks;
		ref_t expected_rc;
		ref_t actual_rc;
	};

	bool flags::check_conformance() {
		if (op == DAMAGE_OP_NONE || op >= DAMAGE_OP_LAST) {
			cerr << "Invalid operation." << endl;
			return false;
		}

		if (!output.size()) {
			cerr << "No output file provided." << endl;
			return false;
		}

		if (!nr_blocks) {
			cerr << "Invalid number of blocks" << endl;
			return false;
		}

		if (op == DAMAGE_OP_CREATE_METADATA_LEAKS &&
		    expected_rc == actual_rc) {
			cerr << "Invalid reference count parameters" << endl;
			return false;
		}

		check_output_file_requirements(output);

		return true;
	}

	int generate_damage(flags const &fs) {
		block_manager::ptr bm = open_bm(fs.output, block_manager::READ_WRITE);
		damage_generator::ptr gen = damage_generator::ptr(new damage_generator(bm));

		switch (fs.op) {
		case flags::DAMAGE_OP_CREATE_METADATA_LEAKS:
			gen->create_metadata_leaks(fs.nr_blocks, fs.expected_rc, fs.actual_rc);
			break;
		default:
			break;
		}

		gen->commit();

		return 0;
	}
}

//----------------------------------------------------------------

thin_generate_damage_cmd::thin_generate_damage_cmd()
	: command("thin_generate_damage")
{
}

void
thin_generate_damage_cmd::usage(std::ostream &out) const
{
}

int
thin_generate_damage_cmd::run(int argc, char **argv)
{
	int c;
	struct flags fs;
	const char *shortopts = "ho:V";
	const struct option longopts[] = {
		{ "help", no_argument, NULL, 'h' },
		{ "output", required_argument, NULL, 'o' },
		{ "create-metadata-leaks", no_argument, NULL, 1 },
		{ "nr-blocks", required_argument, NULL, 1001 },
		{ "expected", required_argument, NULL, 1002 },
		{ "actual", required_argument, NULL, 1003 },
		{ "version", no_argument, NULL, 'V' },
		{ NULL, no_argument, NULL, 0 }
	};

	while ((c = getopt_long(argc, argv, shortopts, longopts, NULL)) != -1) {
		switch(c) {
		case 'h':
			usage(cout);
			break;
		case 'o':
			fs.output = optarg;
			break;
		case 1:
			fs.op = flags::DAMAGE_OP_CREATE_METADATA_LEAKS;
			break;
		case 1001:
			fs.nr_blocks = parse_uint64(optarg, "nr_blocks");
			break;
		case 1002:
			fs.expected_rc = parse_uint64(optarg, "expected");
			break;
		case 1003:
			fs.actual_rc = parse_uint64(optarg, "actual");
			break;
		}
	}

	if (!fs.check_conformance()) {
		usage(cerr);
		return 1;
	}

	return generate_damage(fs);
}

//----------------------------------------------------------------

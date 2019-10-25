// Copyright (C) 2011 Red Hat, Inc. All rights reserved.
//
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

#include <fstream>
#include <iostream>
#include <getopt.h>
#include <libgen.h>

#include "persistent-data/file_utils.h"
#include "thin-provisioning/commands.h"
#include "thin-provisioning/human_readable_format.h"
#include "thin-provisioning/metadata.h"
#include "thin-provisioning/metadata_dumper.h"
#include "thin-provisioning/shared_library_emitter.h"
#include "thin-provisioning/xml_format.h"
#include "version.h"

using namespace boost;
using namespace persistent_data;
using namespace std;
using namespace thin_provisioning;

namespace {
	// FIXME: put the path into the flags
	struct flags {
		flags()
			: format("xml"),
			  repair(false),
			  use_metadata_snap(false) {
		}

		dump_options opts;

		string format;
		bool repair;
		bool use_metadata_snap;
		optional<block_address> snap_location;
	};

	metadata::ptr open_metadata(string const &path, struct flags &flags) {
		block_manager<>::ptr bm = open_bm(path, block_manager<>::READ_ONLY, !flags.use_metadata_snap);
		metadata::ptr md(flags.use_metadata_snap ? new metadata(bm, flags.snap_location) : new metadata(bm, false));

		return md;
	}

	bool begins_with(string const &str, string const &prefix) {
		return str.substr(0, prefix.length()) == prefix;
	}

	emitter::ptr create_emitter(string const &format, ostream &out) {
		emitter::ptr e;

		if (format == "xml")
			e = create_xml_emitter(out);

		else if (format == "human_readable")
			e = create_human_readable_emitter(out);

		else if (begins_with(format, "custom="))
			e = create_custom_emitter(format.substr(7), out);

		else {
			ostringstream msg;
			msg << "unknown format '" << format << "'";
			throw runtime_error(msg.str());
		}

		return e;
	}

        int dump_(string const &path, ostream &out, struct flags &flags) {
                try {
                        emitter::ptr inner = create_emitter(flags.format, out);
                        emitter::ptr e = create_override_emitter(inner, flags.opts.overrides_);

                        if (flags.repair) {
                                auto bm = open_bm(path, block_manager<>::READ_ONLY, true);
                                metadata_repair(bm, e, flags.opts.overrides_);
                        } else {
                                metadata::ptr md = open_metadata(path, flags);
                                metadata_dump(md, e, flags.opts);
                        }

                } catch (std::exception &e) {
                        cerr << e.what() << endl;
                        return 1;
                }

                return 0;
        }

        int dump(string const &path, char const *output, struct flags &flags) {
                if (output) {
                        ofstream out(output);
                        return dump_(path, out, flags);
                } else
                        return dump_(path, cout, flags);
        }

}

//----------------------------------------------------------------

thin_dump_cmd::thin_dump_cmd()
	: command("thin_dump")
{
}

void
thin_dump_cmd::usage(std::ostream &out) const
{
	out << "Usage: " << get_name() << " [options] {device|file}\n"
	    << "Options:\n"
	    << "  {-h|--help}\n"
	    << "  {-f|--format} {xml|human_readable|custom}\n"
	    << "  {-r|--repair}\n"
	    << "  {-m|--metadata-snap} [block#]\n"
	    << "  {-o <xml file>}\n"
	    << "  {--dev-id} <dev-id>\n"
	    << "  {--skip-mappings}\n"
	    << "  {-V|--version}" << endl;
}

int
thin_dump_cmd::run(int argc, char **argv)
{
	int c;
	char const *output = NULL;
	const char shortopts[] = "hm::o:f:rV";
	char *end_ptr;
	block_address metadata_snap = 0;
	uint64_t dev_id;
	struct flags flags;

	const struct option longopts[] = {
		{ "help", no_argument, NULL, 'h'},
		{ "metadata-snap", optional_argument, NULL, 'm' },
		{ "output", required_argument, NULL, 'o'},
		{ "format", required_argument, NULL, 'f' },
		{ "repair", no_argument, NULL, 'r'},
		{ "dev-id", required_argument, NULL, 1 },
		{ "skip-mappings", no_argument, NULL, 2 },
		{ "version", no_argument, NULL, 'V'},
		{ NULL, no_argument, NULL, 0 }
	};

	while ((c = getopt_long(argc, argv, shortopts, longopts, NULL)) != -1) {
		switch(c) {
		case 'h':
			usage(cout);
			return 0;

		case 'f':
			flags.format = optarg;
			break;

		case 'r':
			flags.repair = true;
			break;

		case 'm':
			flags.use_metadata_snap = true;
			if (optarg) {
				// FIXME: deprecate this option
				metadata_snap = strtoull(optarg, &end_ptr, 10);
				if (end_ptr == optarg) {
					cerr << "couldn't parse <metadata-snap>" << endl;
					usage(cerr);
					return 1;
				}

				flags.snap_location = metadata_snap;
			}
			break;

		case 'o':
			output = optarg;
			break;

		case 1:
			dev_id = strtoull(optarg, &end_ptr, 10);
			if (end_ptr == optarg) {
				cerr << "couldn't parse <dev-id>\n";
				usage(cerr);
				return 1;
			}
			flags.opts.select_dev(dev_id);
			break;

		case 2:
			flags.opts.skip_mappings_ = true;
			break;

                case 3:
                        flags.opts.overrides_.transaction_id_ = parse_uint64(optarg, "transaction id");
                        break;

                case 4:
                        flags.opts.overrides_.data_block_size_ = static_cast<uint32_t>(parse_uint64(optarg, "data block size"));
                        break;

                case 5:
                        flags.opts.overrides_.nr_data_blocks_ = parse_uint64(optarg, "nr data blocks");
                        break;

		case 'V':
			cout << THIN_PROVISIONING_TOOLS_VERSION << endl;
			return 0;

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

	return dump(argv[optind], output, flags);
}

//----------------------------------------------------------------

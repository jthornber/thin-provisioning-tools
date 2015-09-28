#include <getopt.h>
#include <iostream>

#include "base/progress_monitor.h"
#include "chunker/cache_stream.h"
#include "chunker/variable_chunk_stream.h"
#include "cruncher/commands.h"
#include "cruncher/compressor.h"
#include "persistent-data/file_utils.h"
#include "version.h"

using namespace base;
using namespace bcache;
using namespace chunker;
using namespace cruncher;
using namespace persistent_data;
using namespace std;

//----------------------------------------------------------------

namespace {
	struct flags {
		string source;
	};

	void usage(ostream &out, string const &cmd) {
		out << "Usage: " << cmd << " [options] {device|file}\n"
		    << "Options:\n"
		    << "  {-h|--help}\n"
		    << "  {-V|--version}" << endl;
	}

}

int crunch(flags const &fs)
{
	compressor comp;
	vector<uint8_t> dest_mem(16 * 1024, 0); // plenty big enough

	block_address block_size = 1024 * 1024 * 4;
	block_address nr_blocks = get_nr_blocks(fs.source, block_size);

	cache_stream stream(fs.source, block_size, 1024 * 1024 * 1024);
	variable_chunk_stream vstream(stream, 4096);

	block_address total_seen(0);
	auto_ptr<progress_monitor> pbar = create_progress_bar("Examining data");

	uint64_t compressed_size(0);

	do {
		// FIXME: use a wrapper class to automate the put()
		chunk const &c = vstream.get();

		compressor::mem_region src(c.mem_.begin, c.mem_.end);
		compressor::mem_region dest(dest_mem.data(), dest_mem.data() + dest_mem.size());

		cerr << "src size = " << src.size()
		     << ", dest size = " << dest.size()
		     << "\n";

		compressed_size += comp.compress(src, dest);

		stream.put(c);

		total_seen += c.len_;
		pbar->update_percent((total_seen * 100) / stream.size());

	} while (stream.next());
	pbar->update_percent(100);

	cerr << "compressed size = " << compressed_size << "\n";

	return 0;
}

int crunch_main(int argc, char **argv)
{
	int c;
	flags fs;

	char const shortopts[] = "hV";
	option const longopts[] = {
		{ "help", no_argument, NULL, 'h'},
		{ "version", no_argument, NULL, 'V'},
		{ NULL, no_argument, NULL, 0 }
	};

	while ((c = getopt_long(argc, argv, shortopts, longopts, NULL)) != -1) {
		switch(c) {
		case 'h':
			usage(cout, basename(argv[0]));
			return 0;

		case 'V':
			cout << THIN_PROVISIONING_TOOLS_VERSION << endl;
			return 0;

		default:
			usage(cerr, basename(argv[0]));
			return 1;
		}
	}

	if (argc == optind) {
		cerr << "No source device/file provided." << endl;
		usage(cerr, basename(argv[0]));
		exit(1);
	}

	fs.source = argv[optind];
	cerr << "crunching " << fs.source << "\n";

	return crunch(fs);
}

base::command cruncher::crunch_cmd("crunch", crunch_main);

//----------------------------------------------------------------

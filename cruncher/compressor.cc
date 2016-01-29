#include "cruncher/compressor.h"

#include <stdexcept>
#include <lz4.h>
#include <lz4hc.h>

using namespace cruncher;
using namespace std;

//----------------------------------------------------------------

namespace {
	unsigned const COMPRESSION_LEVEL = 9;
}

//----------------------------------------------------------------

compressor::compressor()
	: compression_tables_(LZ4_sizeofStateHC() / 4, 0)
{
}

unsigned
compressor::compress(compressor::mem_region const &src,
		     compressor::mem_region &dest)
{
	int r = LZ4_compressHC2_withStateHC(compression_tables_.data(),
		(const char *) src.begin,
		(char *) dest.begin,
		src.size(), COMPRESSION_LEVEL);

	// FIXME: catch OOS
	if (r == 0)
		throw compression_oos_error("out of dest space for compression");

	if (r <= 0)
		throw compression_error("compression failed");

	return r;
}

void
compressor::decompress(compressor::mem_region const &src,
		       compressor::mem_region &dest)
{
	int r = LZ4_decompress_safe((const char *) src.begin,
				    (char *) dest.begin,
				    src.size(), dest.size());
	if (r <= 0)
		throw compression_error("decompression failed.");
}

//----------------------------------------------------------------

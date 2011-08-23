#include "metadata.h"
#include "core_map.h"

#define BOOST_TEST_MODULE MetadataTests
#include <boost/test/included/unit_test.hpp>

using namespace std;
using namespace boost;
using namespace persistent_data;
using namespace thin_provisioning;

//----------------------------------------------------------------

namespace {
	block_address const NR_BLOCKS = 1024;
	block_address const SUPERBLOCK = 0;

	metadata::ptr
	create_metadata() {
		auto tm = create_tm();
		return metadata::ptr(
			new metadata(tm, 0, 128, 1024000, true));
	}
}

//----------------------------------------------------------------

BOOST_AUTO_TEST_CASE(create_metadata_object)
{
	auto m = create_metadata();
}

//----------------------------------------------------------------

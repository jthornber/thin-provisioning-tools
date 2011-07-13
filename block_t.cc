#include "block.h"

#include <iostream>

#define BOOST_TEST_MODULE BlockManagerTests
#include <boost/test/included/unit_test.hpp>

using namespace std;

//----------------------------------------------------------------

namespace {
	template <uint32_t BlockSize>
	void check_all_bytes(typename block_manager<BlockSize>::read_ref const &rr, int v) {
		auto data = rr.data();
		for (unsigned b = 0; b < BlockSize; b++)
			BOOST_CHECK_EQUAL(data[b], v);
	}

	template <uint32_t BlockSize>
	class zero_validator : public block_manager<BlockSize>::validator {
		void check(block_manager<4096>::block const &blk) const {
			for (unsigned b = 0; b < BlockSize; b++)
				if (blk.data_[b] != 0)
					throw runtime_error("validator check zero");
		}

		void prepare(block_manager<4096>::block &blk) const {
			for (unsigned b = 0; b < BlockSize; b++)
				blk.data_[b] = 0;
		}
	};
}

//----------------------------------------------------------------

BOOST_AUTO_TEST_CASE(bad_path)
{
	BOOST_CHECK_THROW(block_manager<4096>("/bogus/bogus/bogus", 1234), runtime_error);
}

BOOST_AUTO_TEST_CASE(out_of_range_access)
{
	block_manager<4096> bm("./test.data", 1024);
	BOOST_CHECK_THROW(bm.read_lock(1024), runtime_error);
}

BOOST_AUTO_TEST_CASE(read_lock_all_blocks)
{
	block_address const nr = 64;
	block_manager<4096> bm("./test.data", nr);

	for (unsigned i = 0; i < nr; i++)
		bm.read_lock(i);
}

BOOST_AUTO_TEST_CASE(write_lock_all_blocks)
{
	block_address const nr = 64;
	block_manager<4096> bm("./test.data", nr);

	for (unsigned i = 0; i < nr; i++)
		bm.write_lock(i);
}

BOOST_AUTO_TEST_CASE(writes_persist)
{
	block_address const nr = 64;
	block_manager<4096> bm("./test.data", nr);

	for (unsigned i = 0; i < nr; i++) {
		auto wr = bm.write_lock(i);
		::memset(wr.data(), i, 4096);
	}

	for (unsigned i = 0; i < nr; i++) {
		auto rr = bm.read_lock(i);
		check_all_bytes<4096>(rr, i % 256);
	}
}

BOOST_AUTO_TEST_CASE(write_lock_zero_zeroes)
{
	block_address const nr = 64;
	block_manager<4096> bm("./test.data", nr);
	check_all_bytes<4096>(bm.write_lock_zero(23), 0);
}

BOOST_AUTO_TEST_CASE(different_block_sizes)
{
	{
		block_manager<4096> bm("./test.data", 64);
		auto rr = bm.read_lock(0);
		BOOST_CHECK_EQUAL(sizeof(rr.data()), 4096);
	}


	{
		block_manager<64 * 1024> bm("./test.data", 64);
		auto rr = bm.read_lock(0);
		BOOST_CHECK_EQUAL(sizeof(rr.data()), 64 * 1024);
	}
}

BOOST_AUTO_TEST_CASE(read_validator_works)
{
	typename block_manager<4096>::block_manager::validator::ptr v(new zero_validator<4096>());
	block_manager<4096> bm("./test.data", 64);
	bm.write_lock_zero(0);
	bm.read_lock(0, v);
}

BOOST_AUTO_TEST_CASE(write_validator_works)
{
	typename block_manager<4096>::block_manager::validator::ptr v(new zero_validator<4096>());
	block_manager<4096> bm("./test.data", 64);
	{
		auto wr = bm.write_lock(0, v);
		::memset(wr.data(), 23, sizeof(wr.data()));
	}

	check_all_bytes<4096>(bm.read_lock(0), 0);
}

//----------------------------------------------------------------

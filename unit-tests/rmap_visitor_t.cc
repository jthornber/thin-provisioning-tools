#include "gmock/gmock.h"

#include "thin-provisioning/rmap_visitor.h"

using namespace std;
using namespace thin_provisioning;
using namespace testing;

//----------------------------------------------------------------

namespace {
	typedef rmap_visitor::rmap_region rmap_region;

	class RMapVisitorTests : public Test {
	public:
		RMapVisitorTests() {
		}

		void visit(uint32_t thin_dev, block_address thin_block, block_address data_block) {
			btree_path path;
			path.push_back(thin_dev);
			path.push_back(thin_block);

			mapping_tree_detail::block_time bt;
			bt.block_ = data_block;
			bt.time_ = 0;

			rmap_v_.visit(path, bt);
		}

		void run() {
			rmap_v_.complete();
		}

		void linear_thins(unsigned nr_thins, unsigned blocks_per_thin) {
			for (uint32_t thin_dev = 0; thin_dev < nr_thins; thin_dev++)
				for (block_address b = 0; b < blocks_per_thin; b++)
					visit(thin_dev, b, thin_dev * blocks_per_thin + b);
		}

		void reverse_linear_thins(unsigned nr_thins, unsigned blocks_per_thin) {
			for (uint32_t thin_dev = 0; thin_dev < nr_thins; thin_dev++)
				for (block_address b = 0; b < blocks_per_thin; b++) {
					block_address base = (nr_thins - thin_dev - 1) * blocks_per_thin;
					visit(thin_dev, b, base + b);
				}
		}

		void check_rmap_size(unsigned expected) {
			ASSERT_THAT(rmap_v_.get_rmap().size(), Eq(expected));
		}

		void check_rmap_at(unsigned index,
				   block_address data_begin, block_address data_end,
				   uint32_t thin_dev, block_address thin_begin) {

			rmap_region expected;

			expected.data_begin = data_begin;
			expected.data_end = data_end;
			expected.thin_dev = thin_dev;
			expected.thin_begin = thin_begin;

			rmap_region actual = rmap_v_.get_rmap().at(index);

			ASSERT_THAT(actual, Eq(expected));
		}

		void add_data_region(block_address data_begin, block_address data_end) {
			rmap_v_.add_data_region(rmap_visitor::region(data_begin, data_end));
		}

		rmap_visitor rmap_v_;
	};

	ostream &operator <<(ostream &out, rmap_region const &r) {
		out << "rmap_region [data_begin = " << r.data_begin
		    << ", data_end = " << r.data_end
		    << ", thin_dev = " << r.thin_dev
		    << ", thin_begin = " << r.thin_begin
		    << endl;
		return out;
	}
}

//----------------------------------------------------------------

TEST_F(RMapVisitorTests, no_mapped_blocks)
{
	run();
	check_rmap_size(0);
}

TEST_F(RMapVisitorTests, no_rmap_regions)
{
	linear_thins(10, 100);

	run();
	check_rmap_size(0);
}

TEST_F(RMapVisitorTests, region_exactly_covers_thin)
{
	add_data_region(0, 100);
	linear_thins(10, 100);

	run();

	check_rmap_size(1);
	check_rmap_at(0, 0, 100, 0, 0);
}

TEST_F(RMapVisitorTests, region_smaller_than_a_thin)
{
	add_data_region(25, 75);
	linear_thins(10, 100);

	run();

	check_rmap_size(1);
	check_rmap_at(0, 25, 75, 0, 25);
}

TEST_F(RMapVisitorTests, region_overlaps_two_thins)
{
	add_data_region(75, 125);
	linear_thins(10, 100);

	run();

	check_rmap_size(2);
	check_rmap_at(0, 75, 100, 0, 75);
	check_rmap_at(1, 100, 125, 1, 0);
}

TEST_F(RMapVisitorTests, two_regions)
{
	add_data_region(75, 125);
	add_data_region(350, 450);
	linear_thins(10, 100);

	run();

	check_rmap_size(4);
	check_rmap_at(0, 75, 100, 0, 75);
	check_rmap_at(1, 100, 125, 1, 0);
	check_rmap_at(2, 350, 400, 3, 50);
	check_rmap_at(3, 400, 450, 4, 0);
}

TEST_F(RMapVisitorTests, overlapping_regions)
{
	add_data_region(25, 75);
	add_data_region(50, 125);
	linear_thins(10, 100);

	run();

	check_rmap_size(2);
	check_rmap_at(0, 25, 100, 0, 25);
	check_rmap_at(1, 100, 125, 1, 0);
}

TEST_F(RMapVisitorTests, rmap_is_sorted)
{
	add_data_region(75, 125);
	add_data_region(350, 450);
	reverse_linear_thins(10, 100);

	run();

	check_rmap_size(4);
	check_rmap_at(0, 75, 100, 9, 75);
	check_rmap_at(1, 100, 125, 8, 0);
	check_rmap_at(2, 350, 400, 6, 50);
	check_rmap_at(3, 400, 450, 5, 0);
}

//----------------------------------------------------------------

#include <chrono>
#include <random>
#include "damage_generator.h"

using namespace thin_provisioning;

//----------------------------------------------------------------

namespace {
	void find_blocks(space_map::ptr sm,
			 block_address nr_blocks,
			 ref_t expected,
			 std::set<block_address> &found) {
		block_address sm_size = sm->get_nr_blocks();
		base::run_set<block_address> visited;
		block_address nr_visited = 0;

		uint64_t rand_seed(std::chrono::high_resolution_clock::now().time_since_epoch().count());
		std::mt19937 rand_engine(rand_seed);

		while (nr_blocks) {
			if (nr_visited == sm_size)
				break;

			block_address b = rand_engine() % sm_size;
			if (visited.member(b))
				continue;

			ref_t c = sm->get_count(b);
			if (c == expected) {
				found.insert(b);
				--nr_blocks;
			}

			visited.add(b);
			++nr_visited;
		}

		if (nr_blocks) {
			ostringstream out;
			out << "cannot find " << (nr_blocks + found.size())
			    << " blocks of ref-count " << expected;
			throw runtime_error(out.str());
		}
	}
}

//----------------------------------------------------------------

damage_generator::damage_generator(block_manager::ptr bm)
{
	md_ = metadata::ptr(new metadata(bm, true));
}

void damage_generator::commit()
{
	md_->commit();
}

void damage_generator::create_metadata_leaks(block_address nr_leaks,
					     ref_t expected, ref_t actual)
{
	std::set<block_address> leaks;
	find_blocks(md_->metadata_sm_, nr_leaks, expected, leaks);

	block_counter bc(true);
	md_->metadata_sm_->count_metadata(bc);
	block_address nr_blocks = md_->metadata_sm_->get_nr_blocks();
	for (block_address b = 0; b < nr_blocks; b++) {
		if (bc.get_count(b))
			md_->tm_->mark_shadowed(b);
	}

	for (auto const &b : leaks)
		md_->metadata_sm_->set_count(b, actual);
}

//----------------------------------------------------------------

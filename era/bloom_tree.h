#ifndef ERA_BLOOM_TREE_H
#define ERA_BLOOM_TREE_H

//----------------------------------------------------------------

namespace era {
}

//----------------------------------------------------------------

#endif


#if 0
	class dm_era {
	public:
		dm_era(block_address nr_blocks)
		: nr_blocks_(nr_blocks),
		  era_base_(0),
		  base_(nr_blocks, false) {
		}

		set<block_address> blocks_written_since(unsigned era) const {

		}

		unsigned get_era() const {
			return era_base_ + eras_.size() - 1;
		}

		void record_write(block_address b) {
			current_era.record_write(b);
		}

		void resize(block_address new_size) {
			nr_blocks_ = new_size;
			push_era();
			base_.resize(new_size, false);
		}

	private:
		era_details &current_era() {
			return eras_.back();
		}

		void need_new_era() {
			// ???
		}

		void push_era() {
			eras_.push_back(era(nr_blocks_));
			if (eras_.size() > 100)
				pop_era();
		}

		void pop_era() {
			era_base_++;



			eras_.pop_front();
		}

		static const unsigned NR_PROBES = 6;

		class era_details {
		public:
			era_details(block_address nr_blocks)
			: nr_blocks_(nr_blocks),
			  f(power_bits(nr_blocks, NR_PROBES)) {
			}

			void record_write(block_address b) {
				f.add(b);
			}

			void add_blocks_written(set<block_address &result) const {
				for (block_address b = 0; b < nr_blocks; b++)
					if (f.test(b))
						result.insert(b);
			}

		private:
			static unsigned power_bits(block_address nr_blocks) {
				// We're expecting 1% of the cache to change per era
				block_address expected_writes = nr_blocks / 100;

				unsigned r = 1;
				while ((1ull << r) < (16 * expected_writes))
					r++;

				return r;
			}

			typedef bloom_filter<block_address_bloom_traits> filter;

			block_address nr_blocks;
			filter f;
		};

		space_map::ptr setup_core_map() {
			space_map::ptr sm(new core_map(NR_BLOCKS));
			sm->inc(SUPERBLOCK);
			return sm;
		}

		block_address nr_blocks_;
		unsigned era_base_;
		vector<bool> base_;
		deque<era_details> eras_;
	};
#endif

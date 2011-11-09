#ifndef BLOCK_COUNTER_H
#define BLOCK_COUNTER_H

#include "block.h"

//----------------------------------------------------------------

namespace persistent_data {
	//----------------------------------------------------------------
	// Little helper class that keeps track of how many times blocks
	// are referenced.
	//----------------------------------------------------------------
	class block_counter {
	public:
		typedef std::map<block_address, unsigned> count_map;

		void inc(block_address b) {
			count_map::iterator it = counts_.find(b);
			if (it == counts_.end())
				counts_.insert(make_pair(b, 1));
			else
				it->second++;
		}

		unsigned get_count(block_address b) const {
			count_map::const_iterator it = counts_.find(b);
			return (it == counts_.end()) ? 0 : it->second;
		}

		count_map const &get_counts() const {
			return counts_;
		}

	private:
		count_map counts_;
	};
}

//----------------------------------------------------------------

#endif

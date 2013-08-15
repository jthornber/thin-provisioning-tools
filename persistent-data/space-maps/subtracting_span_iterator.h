#ifndef SPAN_ITERATOR_H
#define SPAN_ITERATOR_H

#include "persistent-data/space_map.h"
#include "persistent-data/run_set.h"

#include <set>

//----------------------------------------------------------------

namespace persistent_data {
	class subtracting_span_iterator : public space_map::span_iterator {
	public:
		typedef base::run_set<block_address> block_set;
		typedef space_map::span span;

		subtracting_span_iterator(block_address max,
					  span_iterator &sub_it,
					  block_set const &forbidden_blocks)
			: max_(max) {
			for (maybe_span ms = sub_it.first(); ms; ms = sub_it.next())
				runs_.add(ms->first, ms->second);

			block_set bs(forbidden_blocks);
			runs_.negate();
			runs_.merge(bs);
			runs_.negate();
		}

		virtual maybe_span first() {
			current_ = runs_.begin();
			return get_current();
		}

		virtual maybe_span next() {
			if (current_ != runs_.end())
				++current_;

			return get_current();
		}

	private:
		maybe_span get_current() {
			return (current_ == runs_.end()) ?
				maybe_span() :
				maybe_span(std::make_pair(maybe_default(current_->begin_, 0ULL),
							  maybe_default(current_->end_, max_)));
		}

		typedef boost::optional<block_address> maybe;
		static block_address maybe_default(maybe const &m, block_address default_) {
			return m ? *m : default_;
		}

		block_address max_;
		block_set runs_;
		block_set::const_iterator current_;
	};
}

//----------------------------------------------------------------

#endif

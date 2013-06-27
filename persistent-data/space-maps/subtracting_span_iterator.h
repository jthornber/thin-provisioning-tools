#ifndef SPAN_ITERATOR_H
#define SPAN_ITERATOR_H

#include "persistent-data/space_map.h"

#include <set>

//----------------------------------------------------------------

namespace persistent_data {
	// FIXME: rewrite with tests using the run_list stuff.
	class subtracting_span_iterator : public space_map::span_iterator {
	public:
		typedef set<block_address> block_set;
		typedef space_map::span span;

		subtracting_span_iterator(span_iterator &sub_it,
					  block_set const &forbidden_blocks)
			: sub_it_(sub_it),
			  forbidden_blocks_(forbidden_blocks),
			  current_begin_() {
		}

		virtual maybe_span first() {
			current_span_ = sub_it_.first();

			if (current_span_)
				current_begin_ = current_span_->first;

			return next();
		}

		virtual maybe_span next() {
			maybe_span r;

			while (more_spans()) {
				r = next_();
				if (r)
					break;
			}

			return r;
		}

	private:
		bool more_spans() const {
			return current_span_;
		}

		bool span_consumed() const {
			return current_begin_ == current_span_->second;
		}

		bool forbidden_block(block_address b) const {
			return forbidden_blocks_.count(b) > 0;
		}

		void skip_forbidden_blocks() {
			while (!span_consumed() && forbidden_block(current_begin_))
				current_begin_++;
		}

		span get_span() {
			block_address b = current_begin_;

			unsigned loop_count = 0;
			while (!span_consumed() && !forbidden_block(current_begin_) && (loop_count++ < 100))
				current_begin_++;

			return span(b, current_begin_);
		}

		void get_current_span_from_sub_it() {
			current_span_ = sub_it_.next();
			if (current_span_)
				current_begin_ = current_span_->first;
		}

		maybe_span next_() {
			if (span_consumed())
				get_current_span_from_sub_it();

			if (!more_spans())
				return maybe_span();

			skip_forbidden_blocks();

			if (!span_consumed())
				return maybe_span(get_span());
			else
				return maybe_span();
		}

		span_iterator &sub_it_;
		block_set const &forbidden_blocks_;
		maybe_span current_span_;
		block_address current_begin_;
	};
}

//----------------------------------------------------------------

#endif

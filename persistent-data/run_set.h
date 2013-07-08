#ifndef PERSISTENT_DATA_H
#define PERSISTENT_DATA_H

#include "persistent-data/run.h"

#include <algorithm>
#include <set>

//----------------------------------------------------------------

namespace base {
	template <typename T>
	class run_set {
	public:
		void add(T const &b) {
			add(run<T>(b, b + 1));
		}

		void add(T const &b, T const &e) {
			add(run<T>(b, e));
		}

		void add(run<T> const &r_) {
			run<T> r(r_);

			if (runs_.size()) {
				// Correct but slow
				const_iterator it = runs_.cbegin();

				// Skip all blocks that end before r
				while (it != runs_.end() && it->end_ <= r.begin_)
					++it;

				// work out which runs overlap
				if (it != runs_.end()) {
					r.begin_ = min_maybe(it->begin_, r.begin_);
					const_iterator first = it;
					while (it != runs_.end() && it->begin_ < r.end_) {
						r.end_ = max_maybe(it->end_, r.end_);
						++it;
					}

					// remove overlapping runs
					runs_.erase(first, it);
				}
			}

			runs_.insert(r);
		}

		void merge(run_set<T> const &rhs) {
			for (const_iterator it = rhs.begin(); it != rhs.end(); ++it)
				add(*it);
		}

		bool member(T const &v) const {
			if (!runs_.size())
				return false;

			auto it = runs_.lower_bound(run<T>(v));

			if (it->begin_ == v)
				return true;

			it--;

			if (it != runs_.end())
				return it->contains(v);

			return false;
		}

		struct compare_begin {
			bool operator ()(run<T> const &r1, run<T> const &r2) const {
				return r1.begin_ < r2.begin_;
			}
		};

		typedef std::set<run<T>, compare_begin> rset;
		typedef typename rset::const_iterator const_iterator;

		const_iterator begin() const {
			return runs_.begin();
		}

		const_iterator end() const {
			return runs_.end();
		}

	private:
		typedef typename run<T>::maybe maybe;

		static maybe min_maybe(maybe const &m1, maybe const &m2) {
			if (!m1 || !m2)
				return maybe();

			return maybe(std::min<T>(*m1, *m2));
		}

		static maybe max_maybe(maybe const &m1, maybe const &m2) {
			if (!m1 || !m2)
				return maybe();

			return maybe(std::max<T>(*m1, *m2));
		}

		std::set<run<T>, compare_begin> runs_;
	};
}

//----------------------------------------------------------------

#endif

#ifndef RUN_LIST_H
#define RUN_LIST_H

#include <set>

//----------------------------------------------------------------

namespace base {
	template <typename T>
	struct run {
		run(T const &b, T const &e)
			: b_(b),
			  e_(e) {
		}

		bool operator< (run const &rhs) const {
			return b_ < rhs.b_;
		}

		T b_, e_;
	};

	template <typename T>
	class run_list {
	public:
		run_list()
			: invert_(false) {
		}

		void add_run(T const &b, T const &e);
		void sub_run(T const &b, T const &e);
		bool in_run(T const &key) const;

		void invert();
		void add(run_list<T> const &rl);
		void sub(run_list<T> const &rl);

	private:
		bool in_run_(T const &key) const;

		bool invert_;
		std::set<run<T> > runs_;
	};
}

//----------------------------------------------------------------

#include "run_list.tcc"

#endif

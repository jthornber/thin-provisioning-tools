#ifndef PERSISTENT_DATA_RANGE_H
#define PERSISTENT_DATA_RANGE_H

#include <boost/optional.hpp>
#include <ostream>

//----------------------------------------------------------------

namespace base {
	template <typename T>
	class range {
	public:
		typedef boost::optional<T> maybe;

		explicit range(maybe begin = maybe(), maybe end = maybe())
			: begin_(begin),
			  end_(end) {
		}

		bool operator ==(range<T> const &r) const {
			return (begin_ == r.begin_ && end_ == r.end_);
		}

		bool contains(T const &v) const {
			if (begin_ && v < *begin_)
				return false;

			if (end_ && v >= *end_)
				return false;

			return true;
		}

		maybe begin_;
		maybe end_;
	};

	template <typename T>
	std::ostream &
	operator <<(std::ostream &out, range<T> const &r) {
		if (r.begin_)
			out << "[" << *r.begin_;
		else
			out << "[-";

		out << ", ";

		if (r.end_)
			out << *r.end_ << "]";
		else
			out << "-]";

		return out;
	}
}

//----------------------------------------------------------------

#endif

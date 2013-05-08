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

		range(maybe begin = maybe(), maybe end = maybe())
			: begin_(begin),
			  end_(end) {
		}

		bool operator ==(range<T> const &r) const {
			return (begin_ == r.begin_ && end_ == r.end_);
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

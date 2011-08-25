#ifndef ERROR_SET_H
#define ERROR_SET_H

#include <boost/optional.hpp>
#include <boost/shared_ptr.hpp>
#include <list>
#include <iosfwd>
#include <string>

//----------------------------------------------------------------

namespace persistent_data {
	// When checking the metadata for a thin device we don't want to
	// stop at the first error.  Instead should collect as much
	// information as possible.  The errors are hierarchical, so the
	// user can control how much detail is displayed.
	class error_set {
	public:
		typedef boost::shared_ptr<error_set> ptr;

		error_set(std::string const &err);

		std::string const &get_description() const;
		std::list<error_set::ptr> const &get_children() const;
		void add_child(error_set::ptr err);
		void add_child(boost::optional<error_set::ptr> maybe_errs);
		void add_child(std::string const &err);

	private:
		std::string err_;
		std::list<error_set::ptr> children_;
	};

	// The error_selector is a little proxy class used when printing
	// errors to a stream.
	class error_selector {
	public:
		error_selector(error_set::ptr errs, unsigned depth);

		void print(std::ostream &out) const;

	private:
		error_set::ptr errs_;
		unsigned depth_;
	};

	std::ostream &operator << (std::ostream &out, error_selector const &errs);
}

//----------------------------------------------------------------

#endif

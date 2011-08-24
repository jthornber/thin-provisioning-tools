#include "error_set.h"

#include <iostream>

using namespace persistent_data;

//----------------------------------------------------------------

error_set::error_set(std::string const &err)
	: err_(err) {
}

std::string const &
error_set::get_description() const
{
	return err_;
}

std::list<error_set::ptr> const &
error_set::get_children() const
{
	return children_;
}

void
error_set::add_child(error_set::ptr err)
{
	children_.push_back(err);
}

void
error_set::add_child(std::string const &err)
{
	error_set::ptr e(new error_set(err));
	add_child(e);
}

//--------------------------------

namespace {
	void indent_by(std::ostream &out, unsigned indent) {
		for (unsigned i = 0; i < indent; i++)
			out << ' ';
	}

	void print_errs(std::ostream &out, error_set::ptr e, unsigned depth, unsigned indent) {
		if (depth == 0)
			return;

		indent_by(out, indent);

		out << e->get_description() << std::endl;
		if (depth > 1) {
			auto children = e->get_children();
			for (auto it = children.begin(); it != children.end(); ++it)
				print_errs(out, *it, depth - 1, indent + 2);
		}
	}
}

error_selector::error_selector(error_set::ptr errs, unsigned depth)
	: errs_(errs),
	  depth_(depth)
{
}

void
error_selector::print(std::ostream &out) const
{
	print_errs(out, errs_, depth_, 0);
}

std::ostream &
persistent_data::operator << (std::ostream &out, error_selector const &errs)
{
	errs.print(out);
	return out;
}

//----------------------------------------------------------------

// Copyright (C) 2011 Red Hat, Inc. All rights reserved.
//
// This file is part of the thin-provisioning-tools source.
//
// thin-provisioning-tools is free software: you can redistribute it
// and/or modify it under the terms of the GNU General Public License
// as published by the Free Software Foundation, either version 3 of
// the License, or (at your option) any later version.
//
// thin-provisioning-tools is distributed in the hope that it will be
// useful, but WITHOUT ANY WARRANTY; without even the implied warranty
// of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License along
// with thin-provisioning-tools.  If not, see
// <http://www.gnu.org/licenses/>.

#include "error_set.h"

#include <iostream>

using namespace persistent_data;
using namespace std;

//----------------------------------------------------------------

error_set::error_set(string const &err)
	: err_(err) {
}

string const &
error_set::get_description() const
{
	return err_;
}

list<error_set::ptr> const &
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
error_set::add_child(boost::optional<error_set::ptr> maybe_errs)
{
	if (maybe_errs)
		children_.push_back(*maybe_errs);
}

void
error_set::add_child(string const &err)
{
	error_set::ptr e(new error_set(err));
	add_child(e);
}

bool
error_set::empty() const
{
	return !children_.size();
}

//--------------------------------

namespace {
	void indent_by(ostream &out, unsigned indent) {
		for (unsigned i = 0; i < indent; i++)
			out << ' ';
	}

	void print_errs(ostream &out, error_set::ptr e, unsigned depth, unsigned indent) {
		if (depth == 0)
			return;

		indent_by(out, indent);

		out << e->get_description() << endl;
		if (depth > 1) {
			list<error_set::ptr> const &children = e->get_children();
			for (list<error_set::ptr>::const_iterator it = children.begin(); it != children.end(); ++it)
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
error_selector::print(ostream &out) const
{
	print_errs(out, errs_, depth_, 0);
}

ostream &
persistent_data::operator << (ostream &out, error_selector const &errs)
{
	errs.print(out);
	return out;
}

//----------------------------------------------------------------

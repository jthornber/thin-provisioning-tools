#include "base/grid_layout.h"

#include <stdexcept>

using namespace base;
using namespace std;

//----------------------------------------------------------------

grid_layout::grid_layout()
	: nr_fields_(0)
{
	new_row();
}

void
grid_layout::render(ostream &out) const
{
	vector<unsigned> widths;
	calc_field_widths(widths);

	grid::const_iterator row;
	bool newline_needed = false;

	for (row = grid_.begin(); row != grid_.end(); ++row) {
		row::const_iterator col;
		unsigned i;

		if (newline_needed) {
			out << "\n";
			newline_needed = false;
		}

		for (col = row->begin(), i = 0; col != row->end(); ++col, ++i) {
			out << justify(widths[i], *col) << " ";
			newline_needed = true;
		}
	}
}

void
grid_layout::new_row()
{
	grid_.push_back(row());
}

grid_layout::row const &
grid_layout::current_row() const
{
	return grid_.back();
}

grid_layout::row &
grid_layout::current_row()
{
	return grid_.back();
}

void
grid_layout::push_field(string const &s)
{
	current_row().push_back(s);
	nr_fields_ = max<unsigned>(nr_fields_, current_row().size());
}

void
grid_layout::calc_field_widths(vector<unsigned> &widths) const
{
	widths.resize(nr_fields_, 0);

	grid::const_iterator row;
	for (row = grid_.begin(); row != grid_.end(); ++row) {
		row::const_iterator col;
		unsigned i;
		for (col = row->begin(), i = 0; col != row->end(); ++col, ++i)
			widths[i] = max<unsigned>(widths[i], col->length());
	}
}

string
grid_layout::justify(unsigned width, string const &txt) const
{
	if (txt.length() > width)
		throw runtime_error("string field too long, internal error");

	string result(width - txt.length(), ' ');
	result += txt;
	return result;
}

//----------------------------------------------------------------

#ifndef BASE_GRID_LAYOUT_H
#define BASE_GRID_LAYOUT_H

#include <boost/lexical_cast.hpp>
#include <iosfwd>
#include <list>
#include <string>
#include <vector>

//----------------------------------------------------------------

namespace base {
	class grid_layout {
	public:
		typedef std::list<std::string> row;
		typedef std::list<row> grid;

		grid_layout();
		void render(std::ostream &out) const;
		void new_row();

		template <typename T>
		void field(T const &t) {
			push_field(boost::lexical_cast<std::string>(t));
		}

	private:
		row &current_row();
		row const &current_row() const;
		void push_field(std::string const &s);
		void calc_field_widths(std::vector<unsigned> &widths) const;
		std::string justify(unsigned width, std::string const &txt) const;

		grid grid_;
		unsigned nr_fields_;
	};
}

//----------------------------------------------------------------

#endif

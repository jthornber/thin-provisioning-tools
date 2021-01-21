#include <string>

#include "base/output_formatter.h"

using namespace dbg;
using namespace std;

//----------------------------------------------------------------

namespace {
	void indent(int depth, std::ostream &out) {
		for (int i = 0; i < depth * 2; i++)
			out << ' ';
	}
}

//----------------------------------------------------------------

void xml_formatter::output(std::ostream &out,
			   int depth,
			   boost::optional<std::string> name) {
	indent(depth, out);
	out << "<fields";
	if (name && (*name).length())
		out << " id=\"" << *name << "\"";

	/* output non-child fields */
	std::vector<field_type>::const_iterator it;
	for (it = fields_.begin(); it != fields_.end(); ++it) {
		if (string const *s = boost::get<string>(&it->get<1>())) {
			out << " " << it->get<0>() << "=\"" << *s << "\"";
		}
	}

	if (children_.size() == 0) {
		out << " />" << endl;
		return;
	}

	/* output child fields */
	out << ">" << endl;
	for (it = children_.begin(); it != children_.end(); ++it) {
		if (!boost::get<string>(&it->get<1>())) {
			formatter::ptr f = boost::get<formatter::ptr>(it->get<1>());
			f->output(out, depth + 1, it->get<0>());
		}
	}

	indent(depth, out);
	out << "</fields>" << endl;
}

//----------------------------------------------------------------

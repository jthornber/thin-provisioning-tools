#include <string>

#include "dbg-lib/output_formatter.h"

using namespace dbg;
using namespace std;

//----------------------------------------------------------------

namespace {
	class abstract_formatter : public formatter {
		typedef boost::optional<std::string> maybe_string;

		void field(std::string const &name, std::string const &value) {
			fields_.push_back(field_type(name, value));
		}

		void child(std::string const &name, formatter::ptr t) {
			children_.push_back(field_type(name, t));
		}

	protected:
		typedef boost::variant<std::string, ptr> value;
		typedef boost::tuple<std::string, value> field_type;

		std::vector<field_type> fields_;
		std::vector<field_type> children_;
	};

	class xml_formatter : public abstract_formatter {
	public:
		void output(std::ostream &out, int depth = 0,
			    boost::optional<std::string> name = boost::none);
	};

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

formatter::ptr
dbg::create_xml_formatter()
{
	return formatter::ptr(new xml_formatter());
}

//----------------------------------------------------------------

#ifndef DBG_OUTPUT_FORMATTER_H
#define DBG_OUTPUT_FORMATTER_H

#include <boost/lexical_cast.hpp>
#include <boost/optional.hpp>
#include <boost/tuple/tuple.hpp>
#include <boost/variant.hpp>

#include <vector>
#include <string>

//----------------------------------------------------------------

namespace dbg {
	class formatter {
	public:
		typedef std::shared_ptr<formatter> ptr;

		virtual ~formatter() {}

		typedef boost::optional<std::string> maybe_string;

		void field(std::string const &name, std::string const &value) {
			fields_.push_back(field_type(name, value));
		}

		void child(std::string const &name, formatter::ptr t) {
			children_.push_back(field_type(name, t));
		}

		virtual void output(std::ostream &out, int depth = 0,
				    boost::optional<std::string> name = boost::none) = 0;

	protected:
		typedef boost::variant<std::string, ptr> value;
		typedef boost::tuple<std::string, value> field_type;

		std::vector<field_type> fields_;
		std::vector<field_type> children_;
	};

	template <typename T>
	void
	field(formatter &t, std::string const &name, T const &value) {
		t.field(name, boost::lexical_cast<std::string>(value));
	}

	//--------------------------------

	class xml_formatter : public formatter {
	public:
		virtual void output(std::ostream &out, int depth = 0,
				    boost::optional<std::string> name = boost::none);
	};
}

//----------------------------------------------------------------

#endif

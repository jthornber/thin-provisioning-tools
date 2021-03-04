#ifndef DBG_OUTPUT_FORMATTER_H
#define DBG_OUTPUT_FORMATTER_H

#include <boost/lexical_cast.hpp>
#include <boost/optional.hpp>
#include <boost/tuple/tuple.hpp>
#include <boost/variant.hpp>

#include <memory>
#include <vector>
#include <string>

//----------------------------------------------------------------

namespace dbg {
	class formatter {
	public:
		typedef std::shared_ptr<formatter> ptr;

		virtual ~formatter() {}
		virtual void field(std::string const &name, std::string const &value) = 0;
		virtual void child(std::string const &name, formatter::ptr t) = 0;
		virtual void output(std::ostream &out, int depth = 0,
				    boost::optional<std::string> name = boost::none) = 0;
	};

	template <typename T>
	void
	field(formatter &t, std::string const &name, T const &value) {
		t.field(name, boost::lexical_cast<std::string>(value));
	}

	formatter::ptr create_xml_formatter();
}

//----------------------------------------------------------------

#endif

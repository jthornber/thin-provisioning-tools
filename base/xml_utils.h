#ifndef BASE_XML_UTILS_H
#define BASE_XML_UTILS_H

#include <boost/lexical_cast.hpp>
#include <boost/optional.hpp>
#include <expat.h>
#include <map>

using namespace std;

//----------------------------------------------------------------

namespace xml_utils {
	// Simple wrapper to ensure the parser gets freed if an exception
	// is thrown during parsing.
	class xml_parser {
	public:
		xml_parser()
			: parser_(XML_ParserCreate(NULL)) {

			if (!parser_)
				throw runtime_error("couldn't create xml parser");
		}

		~xml_parser() {
			XML_ParserFree(parser_);
		}

		XML_Parser get_parser() {
			return parser_;
		}

	private:
		XML_Parser parser_;
        };

	typedef std::map<std::string, std::string> attributes;

	void build_attributes(attributes &a, char const **attr);

	template <typename T>
	T get_attr(attributes const &attr, string const &key) {
		attributes::const_iterator it = attr.find(key);
		if (it == attr.end()) {
			ostringstream out;
			out << "could not find attribute: " << key;
			throw runtime_error(out.str());
		}

		return boost::lexical_cast<T>(it->second);
	}

	template <typename T>
	boost::optional<T> get_opt_attr(attributes const &attr, string const &key) {
		typedef boost::optional<T> rtype;
		attributes::const_iterator it = attr.find(key);
		if (it == attr.end())
			return rtype();

		return rtype(boost::lexical_cast<T>(it->second));
	}
}

//----------------------------------------------------------------

#endif

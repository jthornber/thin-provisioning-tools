#ifndef BASE_XML_UTILS_H
#define BASE_XML_UTILS_H

#include <base/progress_monitor.h>
#include <boost/lexical_cast.hpp>
#include <boost/optional.hpp>
#include <expat.h>
#include <iosfwd>
#include <map>
#include <stdexcept>

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

		void parse(std::string const &backup_file, bool quiet);

	private:
		size_t get_file_length(string const &file) const;
		unique_ptr<base::progress_monitor> create_monitor(bool quiet);

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

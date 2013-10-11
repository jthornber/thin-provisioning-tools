#ifndef BASE_BASE64_H
#define BASE_BASE64_H

#include <boost/variant.hpp>
#include <string>
#include <vector>

//----------------------------------------------------------------

namespace base {
	std::string base64_encode(std::vector<unsigned char> const &raw);

	// Returns either the decoded data or an error string
	typedef boost::variant<std::vector<unsigned char>, std::string> decoded_or_error;
	decoded_or_error base64_decode(std::string const &encoded);
}

//----------------------------------------------------------------

#endif

#include "xml_utils.h"

//----------------------------------------------------------------

void
xml_utils::build_attributes(attributes &a, char const **attr)
{
	while (*attr) {
		char const *key = *attr;

		attr++;
		if (!*attr) {
			ostringstream out;
			out << "No value given for xml attribute: " << key;
			throw runtime_error(out.str());
		}

		char const *value = *attr;
		a.insert(make_pair(string(key), string(value)));
		attr++;
	}
}


//----------------------------------------------------------------

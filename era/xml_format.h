#ifndef ERA_XML_FORMAT_H
#define ERA_XML_FORMAT_H

#include "emitter.h"

#include <iosfwd>

//----------------------------------------------------------------

namespace era {
	emitter::ptr create_xml_emitter(std::ostream &out);
	void parse_xml(std::istream &in, emitter::ptr e);
}

//----------------------------------------------------------------

#endif

#ifndef XML_FORMAT_H
#define XML_FORMAT_H

#include "emitter.h"

#include <iosfwd>

//----------------------------------------------------------------

namespace thin_provisioning {
	emitter::ptr create_xml_emitter(std::ostream &out);
}

//----------------------------------------------------------------

#endif

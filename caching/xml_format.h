#ifndef CACHE_XML_FORMAT_H
#define CACHE_XML_FORMAT_H

#include "base/progress_monitor.h"
#include "emitter.h"

#include <iosfwd>

//----------------------------------------------------------------

namespace caching {
	emitter::ptr create_xml_emitter(std::ostream &out);
	void parse_xml(std::istream &in, emitter::ptr e,
		       size_t input_len, base::progress_monitor &monitor);
}

//----------------------------------------------------------------

#endif

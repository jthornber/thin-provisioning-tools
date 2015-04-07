#ifndef ERA_XML_FORMAT_H
#define ERA_XML_FORMAT_H

#include "base/progress_monitor.h"
#include "era/emitter.h"

#include <iosfwd>

//----------------------------------------------------------------

namespace era {
	emitter::ptr create_xml_emitter(std::ostream &out);
	void parse_xml(std::string const &backup_file, emitter::ptr e, bool quiet);
}

//----------------------------------------------------------------

#endif

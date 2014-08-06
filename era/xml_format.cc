#include "era/xml_format.h"

#include "base/indented_stream.h"
#include "base/xml_utils.h"

#include <expat.h>

using namespace boost;
using namespace era;
using namespace persistent_data;
using namespace std;
using namespace xml_utils;

//----------------------------------------------------------------

namespace {
	class xml_emitter : public emitter {
	public:
		xml_emitter(ostream &out)
		: out_(out) {
		}

		void begin_superblock(std::string const &uuid,
				      uint32_t block_size,
				      pd::block_address nr_blocks,
				      uint32_t current_era) {
			out_.indent();
			out_ << "<superblock uuid=\"" << uuid << "\""
			     << " block_size=\"" << block_size << "\""
			     << " nr_blocks=\"" << nr_blocks << "\""
			     << " current_era=\"" << current_era << "\">";
			out_ << endl;
			out_.inc();
		}

		void end_superblock() {
			out_.dec();
			out_.indent();
			out_ << "</superblock>" << endl;
		}

		void begin_writeset(uint32_t era, uint32_t nr_bits) {
			out_.indent();
			out_ << "<writeset era=\"" << era << "\""
			     << " nr_bits=\"" << nr_bits << "\">" << endl;
			out_.inc();
		}

		void writeset_bit(uint32_t bit, bool value) {
			out_.indent();
			// FIXME: collect all the bits, then uuencode
			out_ << "<bit bit=\"" << bit << "\" value=\"" << value << "\">" << endl;
		}

		void end_writeset() {
			out_.dec();
			out_.indent();
			out_ << "</writeset>" << endl;
		}

		void begin_era_array() {
			out_.indent();
			out_ << "<era_array>" << endl;
			out_.inc();
		}

		void era(pd::block_address block, uint32_t era) {
			out_.indent();
			out_ << "<era block=\"" << block
			     << "\" era=\"" << era << "\">" << endl;
		}

		void end_era_array() {
			out_.dec();
			out_.indent();
			out_ << "</era_array>" << endl;
		}

	private:
		indented_stream out_;
	};

	//--------------------------------
	// Parser
	//--------------------------------
	void start_tag(void *data, char const *el, char const **attr) {
		emitter *e = static_cast<emitter *>(data);
		attributes a;

		build_attributes(a, attr);

		if (!strcmp(el, "superblock"))
			e->begin_superblock(get_attr<string>(a, "uuid"),
					    get_attr<uint32_t>(a, "block_size"),
					    get_attr<pd::block_address>(a, "nr_blocks"),
					    get_attr<uint32_t>(a, "current_era"));

		else if (!strcmp(el, "writeset"))
			e->begin_writeset(get_attr<uint32_t>(a, "era"),
					  get_attr<uint32_t>(a, "nr_bits"));

		else if (!strcmp(el, "bit"))
			e->writeset_bit(get_attr<uint32_t>(a, "bit"),
					get_attr<bool>(a, "value"));

		else if (!strcmp(el, "era_array"))
			e->begin_era_array();

		else if (!strcmp(el, "era"))
			e->era(get_attr<pd::block_address>(a, "block"),
			       get_attr<uint32_t>(a, "era"));

		else
			throw runtime_error("unknown tag type");
	}

	void end_tag(void *data, const char *el) {
		emitter *e = static_cast<emitter *>(data);

		if (!strcmp(el, "superblock"))
			e->end_superblock();

		else if (!strcmp(el, "writeset"))
			e->end_writeset();

		else if (!strcmp(el, "era_array"))
			e->end_era_array();

		else
			throw runtime_error("unknown tag type");
	}
}

//----------------------------------------------------------------

emitter::ptr
era::create_xml_emitter(std::ostream &out)
{
	return emitter::ptr(new xml_emitter(out));
}

void
era::parse_xml(std::istream &in, emitter::ptr e)
{
	XML_Parser parser = XML_ParserCreate(NULL);
	if (!parser)
		throw runtime_error("couldn't create xml parser");

	XML_SetUserData(parser, e.get());
	XML_SetElementHandler(parser, start_tag, end_tag);

	while (!in.eof()) {
		char buffer[4096];
		in.read(buffer, sizeof(buffer));
		size_t len = in.gcount();
		int done = in.eof();

		if (!XML_Parse(parser, buffer, len, done)) {
			ostringstream out;
			out << "Parse error at line "
			    << XML_GetCurrentLineNumber(parser)
			    << ":\n"
			    << XML_ErrorString(XML_GetErrorCode(parser))
			    << endl;
			throw runtime_error(out.str());
		}
	}
}

//----------------------------------------------------------------

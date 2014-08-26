#include "caching/xml_format.h"

#include "base/base64.h"
#include "base/indented_stream.h"
#include "base/xml_utils.h"

#include <boost/lexical_cast.hpp>

using namespace caching;
using namespace persistent_data;
using namespace std;
using namespace xml_utils;

//----------------------------------------------------------------

namespace {
	//--------------------------------
	// Emitter
	//--------------------------------
	class xml_emitter : public emitter {
	public:
		xml_emitter(ostream &out)
		: out_(out) {
		}

		void begin_superblock(std::string const &uuid,
				      block_address block_size,
				      block_address nr_cache_blocks,
				      std::string const &policy,
				      size_t hint_width) {
			out_.indent();
			out_ << "<superblock uuid=\"" << uuid << "\""
			     << " block_size=\"" << block_size << "\""
			     << " nr_cache_blocks=\"" << nr_cache_blocks << "\""
			     << " policy=\"" << policy << "\""
			     << " hint_width=\"" << hint_width << "\">" << endl;
			out_.inc();
		}

		virtual void end_superblock() {
			out_.dec();
			out_.indent();
			out_ << "</superblock>" << endl;
		}

		virtual void begin_mappings() {
			out_.indent();
			out_ << "<mappings>" << endl;
			out_.inc();
		}

		virtual void end_mappings() {
			out_.dec();
			out_.indent();
			out_ << "</mappings>" << endl;
		}

		virtual void mapping(block_address cblock,
				     block_address oblock,
				     bool dirty) {
			out_.indent();
			out_ << "<mapping"
			     << " cache_block=\"" << cblock << "\""
			     << " origin_block=\"" << oblock << "\""
			     << " dirty=\"" << as_truth(dirty) << "\""
			     << "/>" << endl;
		}

		virtual void begin_hints() {
			out_.indent();
			out_ << "<hints>" << endl;
			out_.inc();
		}

		virtual void end_hints() {
			out_.dec();
			out_.indent();
			out_ << "</hints>" << endl;
		}

		virtual void hint(block_address cblock,
				  vector<unsigned char> const &data) {
			using namespace base;

			out_.indent();
			out_ << "<hint"
			     << " cache_block=\"" << cblock << "\""
			     << " data=\"" << base64_encode(data) << "\""
			     << "/>" << endl;
		}

		virtual void begin_discards() {
			out_.indent();
			out_ << "<discards>" << endl;
			out_.inc();
		}

		virtual void end_discards() {
			out_.dec();
			out_.indent();
			out_ << "</discards>" << endl;
		}

		virtual void discard(block_address dblock_b, block_address dblock_e) {
			out_.indent();
			out_ << "<discard dbegin=\"" << dblock_b << "\""
			     << " dend=\"" << dblock_e << "\"/>" << endl;
		}

	private:
		string as_truth(bool v) const {
			return v ? "true" : "false";
		}

		indented_stream out_;
	};

	//--------------------------------
	// Parser
	//--------------------------------
	void parse_superblock(emitter *e, attributes const &attr) {
		e->begin_superblock(get_attr<string>(attr, "uuid"),
				    get_attr<uint64_t>(attr, "block_size"),
				    get_attr<uint64_t>(attr, "nr_cache_blocks"),
				    get_attr<string>(attr, "policy"),
				    get_attr<size_t>(attr, "hint_width"));
	}

	bool to_bool(string const &str) {
		if (str == "true")
			return true;

		else if (str == "false")
			return false;

		throw runtime_error("bad boolean value");
	}

	void parse_mapping(emitter *e, attributes const &attr) {
		e->mapping(get_attr<uint64_t>(attr, "cache_block"),
			   get_attr<uint64_t>(attr, "origin_block"),
			   to_bool(get_attr<string>(attr, "dirty")));
	}

	void parse_hint(emitter *e, attributes const &attr) {
		using namespace base;

		block_address cblock = get_attr<uint64_t>(attr, "cache_block");
		decoded_or_error doe = base64_decode(get_attr<string>(attr, "data"));
		if (!boost::get<vector<unsigned char> >(&doe)) {
			ostringstream msg;
			msg << "invalid base64 encoding of hint for cache block "
			    << cblock << ": " << boost::get<string>(doe);
			throw runtime_error(msg.str());
		}

		e->hint(cblock, boost::get<vector<unsigned char> >(doe));
	}

	// FIXME: why passing e by ptr?
	void parse_discard(emitter *e, attributes const &attr) {
		e->discard(get_attr<uint64_t>(attr, "dbegin"),
			   get_attr<uint64_t>(attr, "dend"));
	}

	void start_tag(void *data, char const *el, char const **attr) {
		emitter *e = static_cast<emitter *>(data);
		attributes a;

		build_attributes(a, attr);

		if (!strcmp(el, "superblock"))
			parse_superblock(e, a);

		else if (!strcmp(el, "mappings"))
			e->begin_mappings();

		else if (!strcmp(el, "mapping"))
			parse_mapping(e, a);

		else if (!strcmp(el, "hints"))
			e->begin_hints();

		else if (!strcmp(el, "hint"))
			parse_hint(e, a);

		else if (!strcmp(el, "discards"))
			e->begin_discards();

		else if (!strcmp(el, "discard"))
			parse_discard(e, a);

		else
			throw runtime_error("unknown tag type");
	}

	void end_tag(void *data, const char *el) {
		emitter *e = static_cast<emitter *>(data);

		if (!strcmp(el, "superblock"))
			e->end_superblock();

		else if (!strcmp(el, "mappings"))
			e->end_mappings();

		else if (!strcmp(el, "mapping"))
			// do nothing
			;

		else if (!strcmp(el, "hints"))
			e->end_hints();

		else if (!strcmp(el, "hint"))
			// do nothing
			;

		else if (!strcmp(el, "discards"))
			e->end_discards();

		else if (!strcmp(el, "discard"))
			// do nothing
			;

		else
			throw runtime_error("unknown tag close");
	}

}

//----------------------------------------------------------------

caching::emitter::ptr
caching::create_xml_emitter(ostream &out)
{
	return emitter::ptr(new xml_emitter(out));
}

void
caching::parse_xml(istream &in, emitter::ptr e,
		   size_t input_length, base::progress_monitor &monitor)
{
	xml_parser p;

	XML_SetUserData(p.get_parser(), e.get());
	XML_SetElementHandler(p.get_parser(), start_tag, end_tag);

	size_t total = 0;

	while (!in.eof()) {
		char buffer[4096];
		in.read(buffer, sizeof(buffer));
		size_t len = in.gcount();
		int done = in.eof();

		if (!XML_Parse(p.get_parser(), buffer, len, done)) {
			ostringstream out;
			out << "Parse error at line "
			    << XML_GetCurrentLineNumber(p.get_parser())
			    << ":\n"
			    << XML_ErrorString(XML_GetErrorCode(p.get_parser()))
			    << endl;
			throw runtime_error(out.str());
		}

		total += len;
		monitor.update_percent(total * 100 / input_length);
	}
}

//----------------------------------------------------------------

#include "xml_format.h"

#include <boost/lexical_cast.hpp>
#include <expat.h>
#include <iostream>
#include <map>
#include <sstream>
#include <stdexcept>
#include <string.h>

using namespace boost;
using namespace std;
using namespace thin_provisioning;

namespace tp = thin_provisioning;

//----------------------------------------------------------------

namespace {
	//------------------------------------------------
	// XML generator
	//------------------------------------------------
	class xml_emitter : public emitter {
	public:
		xml_emitter(ostream &out)
			: out_(out),
			  indent_(0) {
		}

		void begin_superblock(string const &uuid,
				      uint64_t time,
				      uint64_t trans_id,
				      uint32_t data_block_size) {
			indent();
			out_ << "<superblock uuid=\"" << uuid << "\""
			     << " time=\"" << time << "\""
			     << " transaction=\"" << trans_id << "\""
			     << " data_block_size=\"" << data_block_size << "\">"
			     << endl;
			inc();
		}

		void end_superblock() {
			dec();
			indent();
			out_ << "</superblock>" << endl;
		}

		void begin_device(uint32_t dev_id,
				  uint64_t mapped_blocks,
				  uint64_t trans_id,
				  uint64_t creation_time,
				  uint64_t snap_time) {
			indent();
			out_ << "<device dev_id =\"" << dev_id << "\""
			     << " mapped_blocks=\"" << mapped_blocks << "\""
			     << " transaction=\"" << trans_id << "\""
			     << " creation_time=\"" << creation_time << "\""
			     << " snap_time=\"" << snap_time << "\">" << endl;
			inc();
		}

		void end_device() {
			dec();
			indent();
			out_ << "</device>" << endl;
		}

		void begin_named_mapping(string const &name) {
			indent();
			out_ << "<named_mapping>" << endl;
			inc();
		}

		void end_named_mapping() {
			dec();
			indent();
			out_ << "</named_mapping>" << endl;
		}

		void identifier(string const &name) {
			indent();
			out_ << "<identifier name=\"" << name << "\"/>" << endl;
		}

		void range_map(uint64_t origin_begin, uint64_t data_begin, uint32_t time, uint64_t len) {
			indent();

			out_ << "<range_mapping origin_begin=\"" << origin_begin << "\""
			     << " data_begin=\"" << data_begin << "\""
			     << " length=\"" << len << "\""
			     << " time=\"" << time << "\""
			     << "/>" << endl;
		}

		void single_map(uint64_t origin_block, uint64_t data_block, uint32_t time) {
			indent();

			out_ << "<single_mapping origin_block=\"" << origin_block << "\""
			     << " data_block=\"" << data_block << "\""
			     << " time=\"" << time << "\""
			     << "/>" << endl;
		}

	private:
		void indent() {
			for (unsigned i = 0; i < indent_ * 2; i++)
				out_ << ' ';
		}

		void inc() {
			indent_++;
		}

		void dec() {
			indent_--;
		}

		ostream &out_;
		unsigned indent_;
	};

	//------------------------------------------------
	// XML parser
	//------------------------------------------------
	typedef std::map<string, string> attributes;

	void build_attributes(attributes &a, char const **attr) {
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

	template <typename T>
	T get_attr(attributes const &attr, string const &key) {
		attributes::const_iterator it = attr.find(key);
		if (it == attr.end()) {
			ostringstream out;
			out << "could not find attribute: " << key;
			throw runtime_error(out.str());
		}

		return lexical_cast<T>(it->second);
	}

	void parse_superblock(emitter *e, attributes const &attr) {
		e->begin_superblock(get_attr<string>(attr, "uuid"),
				    get_attr<uint64_t>(attr, "time"),
				    get_attr<uint64_t>(attr, "transaction"),
				    get_attr<uint32_t>(attr, "data_block_size"));
	}

	void parse_device(emitter *e, attributes const &attr) {
		e->begin_device(get_attr<uint32_t>(attr, "dev_id"),
				get_attr<uint64_t>(attr, "mapped_blocks"),
				get_attr<uint64_t>(attr, "transaction"),
				get_attr<uint64_t>(attr, "creation_time"),
				get_attr<uint64_t>(attr, "snap_time"));
	}

	void parse_range_mapping(emitter *e, attributes const &attr) {
		e->range_map(get_attr<uint64_t>(attr, "origin_begin"),
			     get_attr<uint64_t>(attr, "data_begin"),
			     get_attr<uint32_t>(attr, "time"),
			     get_attr<uint64_t>(attr, "length"));
	}

	void parse_single_mapping(emitter *e, attributes const &attr) {
		e->single_map(get_attr<uint64_t>(attr, "origin_block"),
			      get_attr<uint64_t>(attr, "data_block"),
			      get_attr<uint32_t>(attr, "time"));
	}

	void start_tag(void *data, char const *el, char const **attr) {
		emitter *e = static_cast<emitter *>(data);
		attributes a;

		build_attributes(a, attr);

		if (!strcmp(el, "superblock"))
			parse_superblock(e, a);

		else if (!strcmp(el, "device"))
			parse_device(e, a);

		else if (!strcmp(el, "range_mapping"))
			parse_range_mapping(e, a);

		else if (!strcmp(el, "single_mapping"))
			parse_single_mapping(e, a);

		else
			throw runtime_error("unknown tag type");
	}

	void end_tag(void *data, const char *el) {
		emitter *e = static_cast<emitter *>(data);

		if (!strcmp(el, "superblock"))
			e->end_superblock();

		else if (!strcmp(el, "device"))
			e->end_device();

		else if (!strcmp(el, "range_mapping")) {
			// do nothing

		} else if (!strcmp(el, "single_mapping")) {
			// do nothing

		} else
			throw runtime_error("unknown tag close");
	}
}

//----------------------------------------------------------------

tp::emitter::ptr
tp::create_xml_emitter(ostream &out)
{
	return emitter::ptr(new xml_emitter(out));
}

void
tp::parse_xml(std::istream &in, emitter::ptr e)
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

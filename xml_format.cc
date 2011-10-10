#include "xml_format.h"


#include <iostream>

using namespace std;
using namespace thin_provisioning;

namespace tp = thin_provisioning;

//----------------------------------------------------------------

namespace {
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

		void range_map(uint64_t origin_begin, uint64_t data_begin, uint64_t len) {
			indent();

			out_ << "<range_mapping origin_begin=\"" << origin_begin << "\""
			     << " data_begin=\"" << data_begin << "\""
			     << " length=\"" << len << "\"/>"
			     << endl;
		}

		void single_map(uint64_t origin_block, uint64_t data_block) {
			indent();

			out_ << "<single_mapping origin_block=\"" << origin_block << "\""
			     << " data_block=\"" << data_block << "\"/>" << endl;
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
}

//----------------------------------------------------------------

tp::emitter::ptr
tp::create_xml_emitter(ostream &out)
{
	return emitter::ptr(new xml_emitter(out));
}

//----------------------------------------------------------------

#include "era/xml_format.h"

using namespace boost;
using namespace era;
using namespace persistent_data;
using namespace std;

//----------------------------------------------------------------

namespace {
	class xml_emitter : public emitter {
	public:
		xml_emitter(ostream &out)
		: out_(out),
		  indent_(0) {
		}

		void begin_superblock(std::string const &uuid,
				      uint32_t block_size,
				      pd::block_address nr_blocks,
				      uint32_t current_era) {
			indent();
			out_ << "<superblock uuid=\"" << uuid << "\""
			     << " block_size=\"" << block_size << "\""
			     << " nr_blocks=\"" << nr_blocks << "\""
			     << " current_era=\"" << current_era << "\">" << endl;
			inc();
		}

		void end_superblock() {
			dec();
			indent();
			out_ << "</superblock>" << endl;
		}

		void begin_bloom(uint32_t era, uint32_t nr_bits,
				 pd::block_address nr_blocks) {
			indent();
			out_ << "<bloom era=\"" << era << "\""
			     << " nr_bits=\"" << nr_bits << "\""
			     << " nr_blocks=\"" << nr_blocks << "\">" << endl;
			inc();
		}

		void bloom_bit(uint32_t bit, bool value) {
			indent();
			// FIXME: collect all the bits, then uuencode
			out_ << "<bit bit=\"" << bit << "\" value=\"" << value << "\">" << endl;
		}

		void end_bloom() {
			dec();
			indent();
			out_ << "</bloom>" << endl;
		}

		void begin_era_array() {
			indent();
			out_ << "<era_array>" << endl;
			inc();
		}

		void era(pd::block_address block, uint32_t era) {
			indent();
			out_ << "<era block=\"" << block
			     << "\" era=\"" << era << "\">" << endl;
		}

		void end_era_array() {
			dec();
			indent();
			out_ << "</era_array>" << endl;
		}

	private:
		// FIXME: factor out a common class with the thin_provisioning emitter
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

emitter::ptr
era::create_xml_emitter(std::ostream &out)
{
	return emitter::ptr(new xml_emitter(out));
}

//----------------------------------------------------------------

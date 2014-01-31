#include "era/xml_format.h"

#include "base/indented_stream.h"

using namespace boost;
using namespace era;
using namespace persistent_data;
using namespace std;

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
}

//----------------------------------------------------------------

emitter::ptr
era::create_xml_emitter(std::ostream &out)
{
	return emitter::ptr(new xml_emitter(out));
}

//----------------------------------------------------------------

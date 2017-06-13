#include "base/indented_stream.h"
#include "thin-provisioning/emitter.h"

using namespace std;
using namespace thin_provisioning;

//----------------------------------------------------------------

namespace {
	class sexp_emitter : public emitter {
	public:
		sexp_emitter(ostream &out)
		: out_(out) {
		}

		virtual void begin_superblock(std::string const &uuid,
					      uint64_t time,
					      uint64_t trans_id,
					      boost::optional<uint32_t> flags,
					      boost::optional<uint32_t> version,
					      uint32_t data_block_size,
					      uint64_t nr_data_blocks,
					      boost::optional<uint64_t> metadata_snap) {
			open("superblock");
			out_.indent();
			out_ << "((uuid \"" << uuid << "\")\n";
			kv("time", time);
			kv("trans_id", trans_id);
			kv("flags", flags);
			kv("version", version);
			kv("data_block_size", data_block_size);
			kv("nr_data_blocks", nr_data_blocks);
			kv("metadata_snap", metadata_snap);
			out_.indent();
			out_ << ")\n";
		}

		virtual void end_superblock() {
			close();
		}

		virtual void begin_device(uint32_t dev_id,
					  uint64_t mapped_blocks,
					  uint64_t trans_id,
					  uint64_t creation_time,
					  uint64_t snap_time) {
			open("device");
			out_.indent();
			out_ << "((dev_id " << dev_id << ")\n";
			kv("mapped_blocks", mapped_blocks);
			kv("trans_id", trans_id);
			kv("creation_time", creation_time);
			kv("snap_time", snap_time);
			out_.indent();
			out_ << ")\n";
		}

		virtual void end_device() {
			close();
		}

		virtual void begin_named_mapping(std::string const &name) {

		}

		virtual void end_named_mapping() {

		}

		virtual void identifier(std::string const &name) {

		}

		virtual void range_map(uint64_t origin_begin, uint64_t data_begin, uint32_t time, uint64_t len) {
			out_.indent();
			out_ << "(range (origin_begin " << origin_begin
			     << ") (data_begin " << data_begin
			     << ") (time " << time
			     << ") (len " << len << "))\n";
		}

		virtual void single_map(uint64_t origin_block, uint64_t data_block, uint32_t time) {
			out_.indent();
			out_ << "(single (origin_block " << origin_block
			     << ") (data_block " << data_block
			     << ") (time " << time << "))\n";
		}

	private:
		void open(char const *tag) {
			out_.indent();
			out_ << "(" << tag << "\n";
			out_.inc();
		}

		void close() {
			out_.dec();
			out_.indent();
			out_ << ")\n";
		}

		template <typename T>
		void kv(char const *k, T const &v) {
			out_.indent();
			out_ << " (" << k << " " << v << ")\n";
		}

		indented_stream out_;
	};
}

//----------------------------------------------------------------

extern "C" {
	emitter::ptr create_emitter(ostream &out) {
		return emitter::ptr(new sexp_emitter(out));
	}
}

//----------------------------------------------------------------

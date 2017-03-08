#include "thin-provisioning/emitter.h"

using namespace std;
using namespace thin_provisioning;

//----------------------------------------------------------------

namespace {
	template <typename T>
	std::ostream &operator << (ostream &out, boost::optional<T> const &maybe) {
		if (maybe)
			out << *maybe;

		return out;
	}

	class old_emitter : public emitter {
	public:
		old_emitter(ostream &out)
		: out_(out) {
		}

		void begin_superblock(string const &uuid,
				      uint64_t time,
				      uint64_t trans_id,
				      boost::optional<uint32_t> flags,
				      boost::optional<uint32_t> version,
				      uint32_t data_block_size,
				      uint64_t nr_data_blocks,
				      boost::optional<uint64_t> metadata_snap) {
			data_block_size_ = data_block_size;
		}

		void end_superblock() {
		}

		void begin_device(uint32_t dev_id,
				  uint64_t mapped_blocks,
				  uint64_t trans_id,
				  uint64_t creation_time,
				  uint64_t snap_time) {
		}

		void end_device() {
		}

		void begin_named_mapping(string const &name) {
		}

		void end_named_mapping() {
		}

		void identifier(string const &name) {
		}

		void range_map(uint64_t origin_begin, uint64_t data_begin, uint32_t time, uint64_t len) {
			out_ << (data_block_size_ << 9)*origin_begin
			     << ":" << (data_block_size_ << 9)*len
			     << ":" << (data_block_size_ << 9)*data_begin
			     << endl;
		}

		void single_map(uint64_t origin_block, uint64_t data_block, uint32_t time) {
			out_ << (data_block_size_ << 9)*origin_block
			     << ":" << (data_block_size_ << 9)
			     << ":" << (data_block_size_ << 9)*data_block
			     << endl;
		}

	private:
		ostream &out_;
		uint64_t data_block_size_;
	};
}

//----------------------------------------------------------------

extern "C" {
	emitter::ptr create_emitter(ostream &out) {
		return emitter::ptr(new old_emitter(out));
	}
}

//----------------------------------------------------------------

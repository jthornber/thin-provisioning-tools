#include "thin-provisioning/shared_library_emitter.h"

#include <dlfcn.h>
#include <stdexcept>

using namespace std;
using namespace thin_provisioning;

//----------------------------------------------------------------

struct shared_object {
public:
	shared_object(const char *shared_lib) {
		handle_ = dlopen(shared_lib, RTLD_LAZY);
		if (!handle_)
			throw runtime_error(dlerror());

		dlerror();    // Clear any existing error
	}

	virtual ~shared_object() {
		dlclose(handle_);
	}

	void *get_symbol(const char *symbol) {
		void *sym = dlsym(handle_, symbol);

		char *error = dlerror();
		if (error)
			throw runtime_error(error);

		return sym;
	}

	void *handle_;
};

class shared_emitter : public emitter {
public:
	shared_emitter(const char *shared_lib, ostream &out): sobj_(shared_lib) {
		emitter::ptr (*create_fn)(ostream &out);
		create_fn = reinterpret_cast<emitter::ptr (*)(ostream &)>(
				sobj_.get_symbol("create_emitter"));
		inner_ = create_fn(out);
	}

	virtual ~shared_emitter() {
	}

	void begin_superblock(std::string const &uuid,
			      uint64_t time,
			      uint64_t trans_id,
			      boost::optional<uint32_t> flags,
			      boost::optional<uint32_t> version,
			      uint32_t data_block_size,
			      uint64_t nr_data_blocks,
			      boost::optional<uint64_t> metadata_snap) {
		inner_->begin_superblock(uuid,
					 time,
					 trans_id,
					 flags,
					 version,
					 data_block_size,
					 nr_data_blocks,
					 metadata_snap);
	}

	void end_superblock() {
		inner_->end_superblock();
	}

	void begin_device(uint32_t dev_id,
			  uint64_t mapped_blocks,
			  uint64_t trans_id,
			  uint64_t creation_time,
			  uint64_t snap_time) {
		inner_->begin_device(dev_id, mapped_blocks, trans_id, creation_time, snap_time);
	}

	void end_device() {
		inner_->end_device();
	}

	void begin_named_mapping(std::string const &name) {
		inner_->begin_named_mapping(name);
	}

	void end_named_mapping() {
		inner_->end_named_mapping();
	}

	void identifier(std::string const &name) {
		inner_->identifier(name);
	}

	void range_map(uint64_t origin_begin, uint64_t data_begin, uint32_t time, uint64_t len) {
		inner_->range_map(origin_begin, data_begin, time, len);
	}

	void single_map(uint64_t origin_block, uint64_t data_block, uint32_t time) {
		inner_->single_map(origin_block, data_block, time);
	}

	shared_object sobj_;
	emitter::ptr inner_;
};

//----------------------------------------------------------------

emitter::ptr
thin_provisioning::create_custom_emitter(string const &shared_lib, ostream &out)
{
	return emitter::ptr(new shared_emitter(shared_lib.c_str(), out));
}

//----------------------------------------------------------------

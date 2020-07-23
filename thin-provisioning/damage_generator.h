#ifndef METADATA_DAMAGE_GENERATOR_H
#define METADATA_DAMAGE_GENERATOR_H

#include "metadata.h"

//----------------------------------------------------------------

class damage_generator {
public:
	typedef std::shared_ptr<damage_generator> ptr;

	damage_generator(block_manager::ptr bm);
	void commit();
	void create_metadata_leaks(block_address nr_leaks, ref_t expected, ref_t actual);

private:
	thin_provisioning::metadata::ptr md_;
};

//----------------------------------------------------------------

#endif

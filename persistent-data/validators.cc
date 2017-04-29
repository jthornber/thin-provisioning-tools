#include "persistent-data/block.h"
#include "persistent-data/checksum.h"
#include "persistent-data/data-structures/btree_disk_structures.h"
#include "persistent-data/errors.h"
#include "persistent-data/validators.h"

#include <sstream>

using namespace bcache;
using namespace persistent_data;

//----------------------------------------------------------------

namespace {
	using namespace btree_detail;

	struct btree_node_validator : public bcache::validator {
		virtual void check(void const *raw, block_address location) const {
			disk_node const *data = reinterpret_cast<disk_node const *>(raw);
			node_header const *n = &data->header;
			crc32c sum(BTREE_CSUM_XOR);
			sum.append(&n->flags, MD_BLOCK_SIZE - sizeof(uint32_t));
			if (sum.get_sum() != to_cpu<uint32_t>(n->csum)) {
				std::ostringstream out;
				out << "bad checksum in btree node (block " << location << ")";
				throw checksum_error(out.str());
			}

			if (to_cpu<uint64_t>(n->blocknr) != location) {
				std::ostringstream out;
				out << "bad block nr in btree node (block = " << location << ")";
				throw checksum_error(out.str());
			}
		}

		virtual bool check_raw(void const *raw) const {
			disk_node const *data = reinterpret_cast<disk_node const *>(raw);
			node_header const *n = &data->header;
			crc32c sum(BTREE_CSUM_XOR);
			sum.append(&n->flags, MD_BLOCK_SIZE - sizeof(uint32_t));
			if (sum.get_sum() != to_cpu<uint32_t>(n->csum))
				return false;
			return true;
		}

		virtual void prepare(void *raw, block_address location) const {
			disk_node *data = reinterpret_cast<disk_node *>(raw);
			node_header *n = &data->header;
			n->blocknr = to_disk<base::le64, uint64_t>(location);

			crc32c sum(BTREE_CSUM_XOR);
			sum.append(&n->flags, MD_BLOCK_SIZE - sizeof(uint32_t));
			n->csum = to_disk<base::le32>(sum.get_sum());
		}
	};
}

//----------------------------------------------------------------

bcache::validator::ptr persistent_data::create_btree_node_validator()
{
	return bcache::validator::ptr(new btree_node_validator());
}

//----------------------------------------------------------------

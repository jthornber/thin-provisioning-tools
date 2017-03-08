#ifndef PERSISTENT_DATA_DATA_STRUCTURES_BTREE_BASE_VISITOR_H
#define PERSISTENT_DATA_DATA_STRUCTURES_BTREE_BASE_VISITOR_H

#include "persistent-data/data-structures/btree.h"

namespace persistent_data {
	namespace btree_detail {
		template <typename ValueType>
		class noop_value_visitor {
		public:
			virtual void visit(btree_path const &path, ValueType const &v) {
			}
		};
	}
}

#endif

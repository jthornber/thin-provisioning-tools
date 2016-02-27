#ifndef PERSISTENT_DATA_DATA_STRUCTURES_BTREE_KEY_VALUE_EXTRACTOR_H
#define PERSISTENT_DATA_DATA_STRUCTURES_BTREE_KEY_VALUE_EXTRACTOR_H

#include "persistent-data/data-structures/btree_damage_visitor.h"
#include <map>

namespace persistent_data {
	namespace btree_detail {
		template <typename ValueType>
		class key_value_extractor {
			typedef typename std::map<uint64_t, ValueType> MapType;
		public:
			key_value_extractor(MapType &map): map_(map) {
			}

			virtual ~key_value_extractor() {
			}

			virtual void visit(btree_path const &path, ValueType const &v) {
				map_.insert(std::make_pair(path.back(), v));
			}
		private:
			MapType &map_;
		};

		template <unsigned Levels, typename ValueTraits>
		void btree_extract_key_values(btree<Levels, ValueTraits> const &tree,
					      std::map<uint64_t, typename ValueTraits::value_type> &map) {
			typedef key_value_extractor<typename ValueTraits::value_type> KeyValueExtractor;
			KeyValueExtractor kve(map);
			noop_damage_visitor noop_dv;
			btree_detail::btree_damage_visitor<KeyValueExtractor, noop_damage_visitor, Levels, ValueTraits>
				v(kve, noop_dv);
			tree.visit_depth_first(v);
		}
	}
}

#endif

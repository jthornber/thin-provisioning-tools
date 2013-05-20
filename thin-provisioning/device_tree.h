#ifndef THIN_DEVICE_CHECKER_H
#define THIN_DEVICE_CHECKER_H

#include "persistent-data/data-structures/btree.h"
#include "persistent-data/range.h"

#include "thin-provisioning/metadata_disk_structures.h"

//----------------------------------------------------------------

namespace thin_provisioning {
	typedef persistent_data::btree<1, device_details_traits> device_tree;

	namespace device_tree_detail {
		class damage_visitor;

		struct damage {
			virtual ~damage() {}
			virtual void visit(damage_visitor &v) const = 0;
		};

		struct missing_devices : public damage {
			missing_devices(std::string const &desc, range<uint64_t> const &keys);
			virtual void visit(damage_visitor &v) const;

			std::string desc_;
			range<uint64_t> keys_;
		};

		class damage_visitor {
		public:
			virtual ~damage_visitor() {}

			void visit(damage const &d) {
				d.visit(*this);
			}

			virtual void visit(btree_path const &path, missing_devices const &d) = 0;
		};

		// FIXME: need to add some more damage types for bad leaf data

	};

	void check_device_tree(device_tree const &tree,
			       device_tree_detail::damage_visitor &visitor);
}

//----------------------------------------------------------------

#endif

#ifndef THIN_DEVICE_CHECKER_H
#define THIN_DEVICE_CHECKER_H

#include "persistent-data/data-structures/btree.h"
#include "persistent-data/run.h"

//----------------------------------------------------------------

namespace thin_provisioning {
	namespace device_tree_detail {
		struct device_details_disk {
			le64 mapped_blocks_;
			le64 transaction_id_;  /* when created */
			le32 creation_time_;
			le32 snapshotted_time_;
		} __attribute__ ((packed));

		struct device_details {
			uint64_t mapped_blocks_;
			uint64_t transaction_id_;  /* when created */
			uint32_t creation_time_;
			uint32_t snapshotted_time_;
		};

		struct device_details_traits {
			typedef device_details_disk disk_type;
			typedef device_details value_type;
			typedef persistent_data::no_op_ref_counter<device_details> ref_counter;

			static void unpack(device_details_disk const &disk, device_details &value);
			static void pack(device_details const &value, device_details_disk &disk);
		};

		class damage_visitor;

		struct damage {
			virtual ~damage() {}
			virtual void visit(damage_visitor &v) const = 0;
		};

		struct missing_devices : public damage {
			missing_devices(std::string const &desc, run<uint64_t> const &keys);
			virtual void visit(damage_visitor &v) const;

			std::string desc_;
			run<uint64_t> keys_;
		};

		class damage_visitor {
		public:
			typedef boost::shared_ptr<damage_visitor> ptr;

			virtual ~damage_visitor() {}

			void visit(damage const &d) {
				d.visit(*this);
			}

			virtual void visit(missing_devices const &d) = 0;
		};

		// FIXME: need to add some more damage types for bad leaf data

		class device_visitor {
		public:
			virtual ~device_visitor() {}
			virtual void visit(block_address dev_id, device_details const &v) = 0;
		};
	};

	typedef persistent_data::btree<1, device_tree_detail::device_details_traits> device_tree;

	void walk_device_tree(device_tree const &tree,
			      device_tree_detail::device_visitor &dev_v,
			      device_tree_detail::damage_visitor &dv);
	void check_device_tree(device_tree const &tree,
			       device_tree_detail::damage_visitor &visitor);
}

//----------------------------------------------------------------

#endif

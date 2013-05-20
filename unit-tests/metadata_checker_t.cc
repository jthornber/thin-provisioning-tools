#include "gmock/gmock.h"

#include "test_utils.h"

#include "persistent-data/block.h"
#include "thin-provisioning/restore_emitter.h"
#include "thin-provisioning/superblock_checker.h"
#include "thin-provisioning/superblock_validator.h"

#include <boost/noncopyable.hpp>
#include <stdlib.h>
#include <unistd.h>
#include <vector>

using namespace persistent_data;
using namespace std;
using namespace test;
using namespace testing;
using namespace thin_provisioning;

//----------------------------------------------------------------

namespace {
	block_address const BLOCK_SIZE = 4096;
	block_address const NR_BLOCKS = 102400;

	//--------------------------------

	class metadata_builder {
	public:
		metadata_builder(block_manager<>::ptr bm)
			: bm_(bm) {
		}

		void build() {
			metadata::ptr md(new metadata(bm_, metadata::CREATE, 128, 10240));
			emitter::ptr restorer = create_restore_emitter(md);

			restorer->begin_superblock("test-generated", 0, 0, 128, 10240,
						   boost::optional<uint64_t>());

			list<uint32_t>::const_iterator it, end = devices_.end();
			for (it = devices_.begin(); it != end; ++it) {
				restorer->begin_device(*it, 0, 0, 0, 0);
				restorer->end_device();
			}

			restorer->end_superblock();
		}

		void add_device(uint32_t dev) {
			devices_.push_back(dev);
		}

	private:
		block_manager<>::ptr bm_;

		list<uint32_t> devices_;
	};

	class devices_visitor : public detail_tree::visitor {
	public:
		struct node_info {
			typedef boost::shared_ptr<node_info> ptr;

			bool leaf;
			unsigned depth;
			unsigned level;
			block_address b;
			range<uint64_t> keys;
		};

		typedef btree_detail::node_location node_location;
		typedef boost::shared_ptr<devices_visitor> ptr;

		virtual bool visit_internal(node_location const &loc,
					    detail_tree::internal_node const &n) {
			record_node(false, loc, n);
			return true;
		}

		virtual bool visit_internal_leaf(node_location const &loc,
						 detail_tree::internal_node const &n) {
			record_node(true, loc, n);
			return true;
		}


		virtual bool visit_leaf(node_location const &loc,
					detail_tree::leaf_node const &n) {
			record_node(true, loc, n);
			return true;
		}

		virtual void visit_complete() {
		}

		vector<node_info::ptr> const &get_nodes() const {
			return nodes_;
		}

		node_info const &random_node() const {
			if (nodes_.empty())
				throw runtime_error("no nodes in btree");

			return *nodes_[::random() % nodes_.size()];
		}

	private:
		// We rely on the visit order being depth first, lowest to highest.
		template <typename N>
		void record_node(bool leaf, node_location const &loc, N const &n) {
			node_info::ptr ni(new node_info);

			ni->leaf = leaf;
			ni->depth = loc.depth;
			ni->level = loc.level();
			ni->b = n.get_location();

			if (n.get_nr_entries())
				ni->keys = range<uint64_t>(n.key_at(0));
			else {
				if (loc.key)
					ni->keys = range<uint64_t>(*loc.key);
				else
					ni->keys = range<uint64_t>();
			}

			if (last_node_at_depth_.size() > loc.depth) {
				node_info::ptr &last = last_node_at_depth_[loc.depth];

				last->keys.end_ = ni->keys.begin_;
				last_node_at_depth_[loc.depth] = ni;
			} else
				last_node_at_depth_.push_back(ni);

			nodes_.push_back(ni);
		}

		vector<node_info::ptr> nodes_;
		vector<node_info::ptr> last_node_at_depth_;
	};

	//--------------------------------

	class damage_visitor_mock : public metadata_damage_visitor {
	public:
		MOCK_METHOD1(visit, void(super_block_corruption const &));
		MOCK_METHOD1(visit, void(missing_device_details const &));
		MOCK_METHOD1(visit, void(missing_devices const &));
		MOCK_METHOD1(visit, void(missing_mappings const &));
		MOCK_METHOD1(visit, void(bad_metadata_ref_count const &));
		MOCK_METHOD1(visit, void(bad_data_ref_count const &));
		MOCK_METHOD1(visit, void(missing_metadata_ref_counts const &));
		MOCK_METHOD1(visit, void(missing_data_ref_counts const &));
	};

	//--------------------------------

	class MetadataCheckerTests : public Test {
	public:
		MetadataCheckerTests()
			: bm_(create_bm<BLOCK_SIZE>(NR_BLOCKS)),
			  builder_(bm_) {
		}

		metadata_builder &get_builder() {
			return builder_;
		}

		superblock read_superblock() {
			superblock sb;

			block_manager<>::read_ref r = bm_->read_lock(SUPERBLOCK_LOCATION, superblock_validator());
			superblock_disk const *sbd = reinterpret_cast<superblock_disk const *>(&r.data());
			superblock_traits::unpack(*sbd, sb);
			return sb;
		}

		void zero_block(block_address b) {
			::test::zero_block(bm_, b);
		}

		with_temp_directory dir_;
		block_manager<>::ptr bm_;
		metadata_builder builder_;
	};
}

//----------------------------------------------------------------

namespace {
	class SuperBlockCheckerTests : public MetadataCheckerTests {
	public:
		SuperBlockCheckerTests() {
			get_builder().build();
		}

		void corrupt_superblock() {
			zero_block(SUPERBLOCK_LOCATION);
		}
	};
}

TEST_F(SuperBlockCheckerTests, creation_requires_a_block_manager)
{
	superblock_checker sc(bm_);
}

TEST_F(SuperBlockCheckerTests, passes_with_good_superblock)
{
	superblock_checker sc(bm_);
	damage_list_ptr damage = sc.check();
	ASSERT_THAT(damage->size(), Eq(0U));
}

TEST_F(SuperBlockCheckerTests, fails_with_bad_checksum)
{
	corrupt_superblock();

	superblock_checker sc(bm_);
	damage_list_ptr damage = sc.check();
	ASSERT_THAT(damage->size(), Eq(1u));

	metadata_damage::ptr d = *damage->begin();
	ASSERT_THAT(dynamic_cast<super_block_corruption *>(d.get()), NotNull());
}

//----------------------------------------------------------------

namespace {
	class DeviceCheckerTests : public MetadataCheckerTests {
	public:
		block_address devices_root() {
			superblock sb = read_superblock();
			return sb.device_details_root_;
		}

		device_checker &dev_checker() {
			if (!dev_checker_.get())
				dev_checker_.reset(new device_checker(bm_, devices_root()));

			return *dev_checker_;
		}

		void damage_should_include(damage_list_ptr damage, missing_device_details const &md) {
			ASSERT_THAT(damage->size(), Gt(0u));

			damage_visitor_mock v;
			EXPECT_CALL(v, visit(Matcher<missing_device_details const &>(Eq(md))));

			(*damage->begin())->visit(v);
		}

	private:
		auto_ptr<device_checker> dev_checker_;
	};
}

TEST_F(DeviceCheckerTests, create_require_a_block_manager_and_a_root_block)
{
	get_builder().build();
	dev_checker();
}

TEST_F(DeviceCheckerTests, passes_with_valid_metadata_containing_zero_devices)
{
	get_builder().build();
	damage_list_ptr damage = dev_checker().check();
	ASSERT_THAT(damage->size(), Eq(0u));
}

TEST_F(DeviceCheckerTests, passes_with_valid_metadata_containing_some_devices)
{
	metadata_builder &b = get_builder();

	b.add_device(1);
	b.add_device(5);
	b.add_device(76);

	b.build();

	damage_list_ptr damage = dev_checker().check();
	ASSERT_THAT(damage->size(), Eq(0u));
}

TEST_F(DeviceCheckerTests, fails_with_corrupt_root)
{
	get_builder().build();
	zero_block(devices_root());

	damage_list_ptr damage = dev_checker().check();
	damage_should_include(damage, missing_device_details(range64()));
}

TEST_F(DeviceCheckerTests, damaging_some_btree_nodes_results_in_the_correct_devices_being_flagged_as_missing)
{
	metadata_builder &b = get_builder();

	// FIXME: We should optimise the restorer so it clones the mapping
	// tree for zero mapping devices, rather than allocating a new one.
	// It would save allocating a heap of blocks, and more importantly
	// make these tests run much faster.
	for (unsigned i = 0; i < 20000; i++)
		b.add_device(i);

	b.build();

	devices_visitor scanner;
	transaction_manager::ptr tm = open_temporary_tm(bm_);
	detail_tree::ptr devices(new detail_tree(tm, devices_root(),
						 device_details_traits::ref_counter()));
	devices->visit_depth_first(scanner);

	devices_visitor::node_info n = scanner.random_node();
	zero_block(n.b);

	damage_list_ptr damage = dev_checker().check();
	damage_should_include(damage, missing_device_details(range64(n.keys)));
}

//----------------------------------------------------------------

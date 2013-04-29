#include "gmock/gmock.h"

#include "test_utils.h"

#include "persistent-data/block.h"
#include "thin-provisioning/device_checker.h"
#include "thin-provisioning/restore_emitter.h"
#include "thin-provisioning/superblock_checker.h"
#include "thin-provisioning/superblock_validator.h"

#include <unistd.h>

using namespace persistent_data;
using namespace std;
using namespace test;
using namespace testing;
using namespace thin_provisioning;

//----------------------------------------------------------------

namespace {
	block_address const BLOCK_SIZE = 4096;
	block_address const NR_BLOCKS = 10240;

	// FIXME: move to utils
	class with_directory {
	public:
		with_directory(std::string const &path)
			: old_path_(pwd()) {
			chdir(path);
		}

		~with_directory() {
			chdir(old_path_);
		}

	private:
		std::string pwd() const {
			char buffer[PATH_MAX];
			char *ptr = getcwd(buffer, sizeof(buffer));
			if (!ptr) {
				// FIXME: still need a standard syscall failed exception
				throw std::runtime_error("getcwd failed");
			}

			return ptr;
		}

		void chdir(std::string const &path) {
			int r = ::chdir(path.c_str());
			if (r < 0)
				throw std::runtime_error("chdir failed");
		}

		std::string old_path_;
		std::string new_path_;
	};

	class with_temp_directory {
	public:
		with_temp_directory() {
			std::string name("./tmp");

			rm_rf(name);
			mkdir(name);

			dir_.reset(new with_directory(name));
		}

	private:
		void rm_rf(std::string const &name) {
			std::string cmd("rm -rf ");
			cmd += name;
			system(cmd);
		}

		void mkdir(std::string const &name) {
			std::string cmd("mkdir ");
			cmd += name;
			system(cmd);
		}

		void system(std::string const &cmd) {
			int r = ::system(cmd.c_str());
			if (r < 0)
				throw std::runtime_error("system failed");
		}

		std::auto_ptr<with_directory> dir_;
	};

	//--------------------------------

	class metadata_builder {
	public:
		metadata_builder(block_manager<>::ptr bm)
			: bm_(bm) {
		}

		void build() {
			metadata::ptr md(new metadata(bm_, metadata::CREATE, 128, 10240));
			emitter::ptr restorer = create_restore_emitter(md);

			restorer->begin_superblock("test-generated", 0, 0, 128, 10240, boost::optional<uint64_t>());

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
			: bm_(create_bm<BLOCK_SIZE>()),
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

		device_checker::ptr mk_checker() {
			return device_checker::ptr(new device_checker(bm_, devices_root()));
		}
	};
}

TEST_F(DeviceCheckerTests, create_require_a_block_manager_and_a_root_block)
{
	get_builder().build();
	mk_checker();
}

TEST_F(DeviceCheckerTests, passes_with_valid_metadata_containing_zero_devices)
{
	get_builder().build();
	damage_list_ptr damage = mk_checker()->check();
	ASSERT_THAT(damage->size(), Eq(0u));
}

TEST_F(DeviceCheckerTests, passes_with_valid_metadata_containing_some_devices)
{
	metadata_builder &b = get_builder();

	b.add_device(1);
	b.add_device(5);
	b.add_device(76);

	b.build();

	damage_list_ptr damage = mk_checker()->check();
	ASSERT_THAT(damage->size(), Eq(0u));
}

TEST_F(DeviceCheckerTests, fails_with_corrupt_root)
{
	get_builder().build();
	zero_block(devices_root());

	damage_list_ptr damage = mk_checker()->check();
	ASSERT_THAT(damage->size(), Eq(1u));

	damage_visitor_mock v;
	EXPECT_CALL(v, visit(Matcher<missing_device_details const &>(Eq(missing_device_details(range64())))));

	(*damage->begin())->visit(v);
}

//----------------------------------------------------------------

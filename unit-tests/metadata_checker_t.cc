#include "gmock/gmock.h"

#include "test_utils.h"

#include "persistent-data/block.h"
#include "thin-provisioning/restore_emitter.h"
#include "thin-provisioning/superblock_checker.h"

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
			restorer->end_superblock();
		}

		// FIXME: add methods to specify volumes with particular
		// mapping patterns.

	private:
		block_manager<>::ptr bm_;
	};

	class SuperBlockCheckerTests : public Test {
	public:
		SuperBlockCheckerTests()
			: bm_(create_bm<BLOCK_SIZE>()) {

			metadata_builder builder(bm_);
			builder.build();
		}

		void corrupt_superblock() {
			zero_block(bm_, SUPERBLOCK_LOCATION);
		}

		with_temp_directory dir_;
		block_manager<>::ptr bm_;
	};
}

//----------------------------------------------------------------

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

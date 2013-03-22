#include "gmock/gmock.h"
#include "gtest/gtest.h"

//----------------------------------------------------------------

namespace {
	class MyClass {

	};
}

//----------------------------------------------------------------

TEST(MyClass, can_construct)
{
	MyClass c;
}

//----------------------------------------------------------------

int main(int argc, char **argv)
{
	testing::InitGoogleTest(&argc, argv);
	return RUN_ALL_TESTS();
}

//----------------------------------------------------------------

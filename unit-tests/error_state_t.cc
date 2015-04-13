#include "gmock/gmock.h"
#include "base/error_state.h"

using namespace base;
using namespace std;
using namespace testing;

//----------------------------------------------------------------

TEST(ErrorStateTests, stream_operator)
{
	{
		error_state err = NO_ERROR;
		err << NO_ERROR << FATAL;
		ASSERT_THAT(err, Eq(FATAL));
	}

	{
		error_state err = NO_ERROR;
		err << NO_ERROR << NON_FATAL;
		ASSERT_THAT(err, Eq(NON_FATAL));
	}

	{
		error_state err = NO_ERROR;
		err << NO_ERROR << FATAL << NO_ERROR << NON_FATAL;
		ASSERT_THAT(err, Eq(FATAL));
	}
}

//----------------------------------------------------------------

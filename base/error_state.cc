#include "base/error_state.h"

//----------------------------------------------------------------

base::error_state
base::combine_errors(error_state lhs, error_state rhs) {
	switch (lhs) {
	case NO_ERROR:
		return rhs;

	case NON_FATAL:
		return (rhs == FATAL) ? FATAL : lhs;

	default:
		return lhs;
	}
}

//----------------------------------------------------------------

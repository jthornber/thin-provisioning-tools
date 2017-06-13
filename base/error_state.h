#ifndef BASE_ERROR_STATE_H
#define BASE_ERROR_STATE_H

//----------------------------------------------------------------

namespace base {
	enum error_state {
		NO_ERROR,
		NON_FATAL,	// eg, lost blocks
		FATAL		// needs fixing before pool can be activated
	};

	error_state combine_errors(error_state lhs, error_state rhs);

	inline error_state &operator <<(error_state &err, error_state rhs) {
		err = combine_errors(err, rhs);
		return err;
	}
}

//----------------------------------------------------------------

#endif

#ifndef BASE_BITS_H
#define BASE_BITS_H

//----------------------------------------------------------------

namespace base {
	template <typename T>
	bool test_bit(T flag, unsigned bit) {
		return flag & (1 << bit);
	}

	template <typename T>
	void set_bit(T &flag, unsigned bit) {
		flag = flag | (1 << bit);
	}

	template <typename T>
	void clear_bit(T &flag, unsigned bit) {
		flag = flag & ~(1 << bit);
	}
}

//----------------------------------------------------------------

#endif


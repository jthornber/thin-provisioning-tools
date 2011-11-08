#ifndef DELETER_H
#define DELETER_H

//----------------------------------------------------------------

namespace utils {
	template <typename T>
	struct deleter {
		void operator()(T *t) {
			delete t;
		}
	};
}

//----------------------------------------------------------------

#endif

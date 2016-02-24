#ifndef BASE_CONTAINER_OF_H
#define BASE_CONTAINER_OF_H

#include <stdlib.h>

//----------------------------------------------------------------

namespace base {
	template<class P, class M>
	size_t offsetof__(const M P::*member)
	{
		return (size_t) &( reinterpret_cast<P*>(0)->*member);
	}

	template<class P, class M>
	P *container_of(M *ptr, M const P::*member)
	{
		return (P *)((char *)(ptr) - offsetof__(member));
	}
}

//----------------------------------------------------------------

#endif

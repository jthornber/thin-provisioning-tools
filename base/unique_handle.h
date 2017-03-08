#ifndef BASE_UNIQUE_HANDLE_H
#define BASE_UNIQUE_HANDLE_H

#include <list>
#include <memory>
#include <unistd.h>

//----------------------------------------------------------------

namespace base {
	template <typename T, T TNul = T()>
	class unique_handle
	{
	public:
		unique_handle(std::nullptr_t = nullptr)
			: id_(TNul) {
		}

		unique_handle(T x)
		: id_(x) {
		}

		explicit operator bool() const {
			return id_ != TNul;
		}

		operator T&() {
			return id_;
		}

		operator T() const {
			return id_;
		}

		T *operator&() {
			return &id_;
		}

		const T *operator&() const {
			return &id_;
		}

		friend bool operator == (unique_handle a, unique_handle b) { return a.id_ == b.id_; }
		friend bool operator != (unique_handle a, unique_handle b) { return a.id_ != b.id_; }
		friend bool operator == (unique_handle a, std::nullptr_t) { return a.id_ == TNul; }
		friend bool operator != (unique_handle a, std::nullptr_t) { return a.id_ != TNul; }
		friend bool operator == (std::nullptr_t, unique_handle b) { return TNul == b.id_; }
		friend bool operator != (std::nullptr_t, unique_handle b) { return TNul != b.id_; }

	private:
		T id_;
	};

	//--------------------------------

	struct fd_deleter {
		typedef unique_handle<int, -1> pointer;
		void operator()(pointer p) {
			::close(p);
		}
	};
	typedef std::unique_ptr<int, fd_deleter> unique_fd;
}

//----------------------------------------------------------------

#endif

#ifndef BASE_INDENTED_STREAM_H
#define BASE_INDENTED_STREAM_H

#include <iostream>

//----------------------------------------------------------------

namespace {
	class indented_stream {
	public:
		indented_stream(std::ostream &out)
		: out_(out),
		  indent_(0) {
		}

		void indent() {
			for (unsigned i = 0; i < indent_ * 2; i++)
				out_ << ' ';
		}

		void inc() {
			indent_++;
		}

		void dec() {
			indent_--;
		}

		template <typename T>
		indented_stream &operator <<(T const &t) {
			out_ << t;
			return *this;
		}

		indented_stream &operator <<(std::ostream &(*fp)(std::ostream &)) {
			out_ << fp;
			return *this;
		}

	private:
		std::ostream &out_;
		unsigned indent_;
	};
}

//----------------------------------------------------------------

#endif

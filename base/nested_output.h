#ifndef BASE_NESTED_OUTPUT_H
#define BASE_NESTED_OUTPUT_H

#include <iostream>

//----------------------------------------------------------------

namespace base {
	class end_message {};

	class nested_output {
	public:
		nested_output(std::ostream &out, unsigned step)
			: out_(out),
			  step_(step),
			  beginning_of_line_(true),
			  enabled_(true),
			  indent_(0) {
		}

		template <typename T>
		nested_output &operator <<(T const &t) {
			if (beginning_of_line_) {
				beginning_of_line_ = false;
				indent();
			}

			if (enabled_)
				out_ << t;

			return *this;
		}

		nested_output &operator <<(end_message const &m) {
			beginning_of_line_ = true;

			if (enabled_)
				out_ << std::endl;

			return *this;
		}

		void inc_indent() {
			indent_ += step_;
		}

		void dec_indent() {
			indent_ -= step_;
		}

		struct nest {
			nest(nested_output &out)
			: out_(out) {
				out_.inc_indent();
			}

			~nest() {
				out_.dec_indent();
			}

			nested_output &out_;
		};

		nest push() {
			return nest(*this);
		}

		void enable() {
			enabled_ = true;
		}

		void disable() {
			enabled_ = false;
		}

	private:
		void indent() {
			if (enabled_)
				for (unsigned i = 0; i < indent_; i++)
					out_ << ' ';
		}

		std::ostream &out_;
		unsigned step_;

		bool beginning_of_line_;
		bool enabled_;
		unsigned indent_;
	};
}

//----------------------------------------------------------------

#endif

#include "base/progress_monitor.h"

#include <iostream>

//----------------------------------------------------------------

namespace {
	using namespace std;

	class progress_bar : public base::progress_monitor {
	public:
		progress_bar(string const &title)
			: title_(title),
			  progress_width_(50),
			  spinner_(0) {

			update_percent(0);
		}
		~progress_bar() {
			cout << "\n";
		}

		void update_percent(unsigned p) {
			unsigned nr_equals = max<unsigned>(progress_width_ * p / 100, 1);
			unsigned nr_spaces = progress_width_ - nr_equals;

			cout << title_ << ": [";

			for (unsigned i = 0; i < nr_equals - 1; i++)
				cout << '=';

			if (nr_equals < progress_width_)
				cout << '>';
			else
				cout << "=";

			for (unsigned i = 0; i < nr_spaces; i++)
				cout << ' ';

			cout << "] " << spinner_char(p) << " " << p << "%\r" << flush;

			spinner_++;
		}

	private:
		char spinner_char(unsigned p) const {
			if (p == 100)
				return ' ';

			char cs[] = {'|', '/', '-', '\\'};

			unsigned index = spinner_ % sizeof(cs);
			return cs[index];
		}

		std::string title_;
		unsigned progress_width_;
		unsigned spinner_;
	};

	class quiet_progress : public base::progress_monitor {
	public:
		void update_percent(unsigned p) {
		}
	};

}

//----------------------------------------------------------------

std::unique_ptr<base::progress_monitor>
base::create_progress_bar(std::string const &title)
{
	return unique_ptr<progress_monitor>(new progress_bar(title));
}

std::unique_ptr<base::progress_monitor>
base::create_quiet_progress_monitor()
{
	return unique_ptr<progress_monitor>(new quiet_progress());
}

//----------------------------------------------------------------

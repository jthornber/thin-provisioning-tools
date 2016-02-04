#ifndef BASE_PROGRESS_MONITOR_H
#define BASE_PROGRESS_MONITOR_H

#include <boost/shared_ptr.hpp>
#include <memory>
#include <string>

//----------------------------------------------------------------

namespace base {
	class progress_monitor {
	public:
		virtual ~progress_monitor() {}

		virtual void update_percent(unsigned) = 0;
	};

	std::unique_ptr<progress_monitor> create_progress_bar(std::string const &title);
	std::unique_ptr<progress_monitor> create_quiet_progress_monitor();
}

//----------------------------------------------------------------

#endif

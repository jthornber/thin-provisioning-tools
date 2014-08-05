#ifndef BASE_PROGRESS_MONITOR_H
#define BASE_PROGRESS_MONITOR_H

#include <boost/shared_ptr.hpp>
#include <string>

//----------------------------------------------------------------

namespace base {
	class progress_monitor {
	public:
		typedef boost::shared_ptr<progress_monitor> ptr;

		virtual ~progress_monitor() {}

		virtual void update_percent(unsigned) = 0;
	};

	progress_monitor::ptr create_progress_bar(std::string const &title);
	progress_monitor::ptr create_quiet_progress_monitor();
}

//----------------------------------------------------------------

#endif

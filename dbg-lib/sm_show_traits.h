#ifndef DBG_SM_SHOW_TRAITS_H
#define DBG_SM_SHOW_TRAITS_H

#include "dbg-lib/output_formatter.h"
#include "persistent-data/space-maps/disk_structures.h"

//----------------------------------------------------------------

namespace dbg {
	class index_entry_show_traits {
	public:
		typedef persistent_data::sm_disk_detail::index_entry_traits value_trait;

		static void show(dbg::formatter::ptr f, std::string const &key,
				 persistent_data::sm_disk_detail::index_entry const &value);
	};

	class sm_root_show_traits {
	public:
		typedef persistent_data::sm_disk_detail::sm_root_traits value_trait;

		static void show(dbg::formatter::ptr f, std::string const &key,
				 persistent_data::sm_disk_detail::sm_root const &value);
	};
}

//----------------------------------------------------------------

#endif
